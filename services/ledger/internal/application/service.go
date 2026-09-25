package application

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"sort"
	"strconv"
	"time"
	_ "time/tzdata"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

var nairobi = func() *time.Location {
	loc, err := time.LoadLocation("Africa/Nairobi")
	if err != nil {
		panic(err)
	}
	return loc
}()

func Bucket(now time.Time) string { return now.In(nairobi).Format("2006-01-02") }

func (c Command) Payload() any {
	switch c.Kind {
	case CreateAccount:
		return c.Create
	case TransferInternal:
		return c.Transfer
	case ReservePayout:
		return c.Reserve
	case MarkHoldExposed:
		return c.Expose
	case CapturePayout:
		return c.Capture
	case ReleasePayout:
		return c.Release
	case SetAccountControl:
		return c.Control
	}
	return nil
}
func (c Command) PaymentID() domain.ID {
	switch c.Kind {
	case TransferInternal:
		return c.Transfer.Terms.PaymentID
	case ReservePayout:
		return c.Reserve.Terms.PaymentID
	case MarkHoldExposed:
		return c.Expose.Ref.PaymentID
	case CapturePayout:
		return c.Capture.Ref.PaymentID
	case ReleasePayout:
		return c.Release.Ref.PaymentID
	}
	return ""
}
func (c Command) HoldID() domain.ID {
	switch c.Kind {
	case ReservePayout:
		return c.Reserve.HoldID
	case MarkHoldExposed:
		return c.Expose.Ref.HoldID
	case CapturePayout:
		return c.Capture.Ref.HoldID
	case ReleasePayout:
		return c.Release.Ref.HoldID
	}
	return ""
}
func (c Command) Permission() string {
	switch c.Kind {
	case CreateAccount:
		return "provision"
	case SetAccountControl:
		return "control"
	case CapturePayout:
		return "resolve"
	case ReleasePayout:
		if c.Release != nil && c.Release.Reason == "FINAL_FAILURE" {
			return "resolve"
		}
	}
	return "spend"
}
func validTerms(t Terms) bool {
	g := t.Grant.Claims
	return t.PaymentID.Valid() && t.SourceID.Valid() && t.BeneficiaryID.Valid() &&
		t.PolicyID.Valid() && t.QuoteID.Valid() && t.Principal.Principal() == nil && t.Currency == "KES" &&
		g.ID.Valid() && g.SubjectID.Valid() && len(g.Issuer) > 0 && len(g.Issuer) <= 64 &&
		len(t.Grant.Signature) <= 128 && !g.NotBefore.IsZero() && !g.ExpiresAt.IsZero()
}
func validRef(r HoldRef) bool {
	return r.PaymentID.Valid() && r.HoldID.Valid() && r.AttemptID.Valid() && r.ExpectedVersion > 0
}
func (c Command) Validate() error {
	if !c.BookID.Valid() || !c.OperationID.Valid() {
		return ErrInvalid
	}
	count := 0
	for _, present := range []bool{c.Create != nil, c.Transfer != nil, c.Reserve != nil, c.Expose != nil, c.Capture != nil, c.Release != nil, c.Control != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return ErrInvalid
	}
	valid := false
	switch c.Kind {
	case CreateAccount:
		valid = c.Create != nil && c.Create.AccountID.Valid() && c.Create.OwnerID.Valid() && c.Create.Purpose == domain.Wallet
	case TransferInternal:
		valid = c.Transfer != nil && validTerms(c.Transfer.Terms)
	case ReservePayout:
		valid = c.Reserve != nil && validTerms(c.Reserve.Terms) && c.Reserve.HoldID.Valid() && c.Reserve.AttemptID.Valid() && c.Reserve.PoolID.Valid() && !c.Reserve.ExpiresAt.IsZero()
	case MarkHoldExposed:
		valid = c.Expose != nil && validRef(c.Expose.Ref) && c.Expose.GrantID.Valid()
	case CapturePayout:
		valid = c.Capture != nil && validRef(c.Capture.Ref) && c.Capture.Evidence.Claims.ID.Valid() && len(c.Capture.Evidence.Signature) <= 128
	case ReleasePayout:
		if c.Release != nil && validRef(c.Release.Ref) {
			r := c.Release
			valid = (r.Reason == "CANCEL" || r.Reason == "EXPIRY") && r.Evidence == nil
			if r.Reason == "FINAL_FAILURE" {
				valid = r.Evidence != nil && r.Evidence.Claims.ID.Valid() && len(r.Evidence.Signature) <= 128
			}
		}
	case SetAccountControl:
		if c.Control != nil {
			s := c.Control
			valid = s.Key.ID.Valid() && (s.Key.Kind == "SUBJECT" || s.Key.Kind == "ACCOUNT") && s.ExpectedVersion > 0 && len(s.Reason) > 0 && len(s.Reason) <= 64
		}
	}
	if !valid {
		return ErrInvalid
	}
	return nil
}

func (c Command) Material() ([]byte, error) {
	data, err := json.Marshal(c.Payload())
	if err != nil {
		return nil, ErrInvalid
	}
	var body any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err = decoder.Decode(&body); err != nil {
		return nil, ErrInvalid
	}
	// Signatures are replaceable proof/transport material, never persisted in the
	// canonical financial instruction. Their signed claims remain bound.
	var removeSignatures func(any)
	removeSignatures = func(v any) {
		switch obj := v.(type) {
		case map[string]any:
			delete(obj, "signature")
			for _, child := range obj {
				removeSignatures(child)
			}
		case []any:
			for _, child := range obj {
				removeSignatures(child)
			}
		}
	}
	removeSignatures(body)
	data, err = Canonical(map[string]any{"schema_version": "1", "book_id": c.BookID, "kind": c.Kind, "body": body})
	if err != nil || len(data) > 16384 {
		return nil, ErrInvalid
	}
	return data, nil
}

type Service struct {
	store Store
	trust Trust
	now   func() time.Time
}

func New(store Store, trust Trust, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	// Key configuration is immutable from the runtime's perspective.
	copyTrust := Trust{GrantKeys: map[string]ed25519.PublicKey{}, EvidenceKeys: map[string]ed25519.PublicKey{}}
	for id, key := range trust.GrantKeys {
		copyTrust.GrantKeys[id] = append(ed25519.PublicKey(nil), key...)
	}
	for id, key := range trust.EvidenceKeys {
		copyTrust.EvidenceKeys[id] = append(ed25519.PublicKey(nil), key...)
	}
	return &Service{store: store, trust: copyTrust, now: now}
}
func receiptAccess(c Caller, r Receipt) bool {
	for _, subject := range r.SubjectIDs {
		if !c.AllowsSubject(subject) {
			return false
		}
	}
	return true
}
func (s *Service) Execute(ctx context.Context, caller Caller, cmd Command) (Receipt, error) {
	if err := cmd.Validate(); err != nil {
		return Receipt{}, err
	}
	if !caller.Allows(cmd.BookID, cmd.Permission()) {
		return Receipt{}, ErrPermission
	}
	canonical, err := cmd.Material()
	if err != nil {
		return Receipt{}, err
	}
	journalID, eventID := domain.NewID(), domain.NewID()
	return s.store.Transact(ctx, func(tx Transaction) (Receipt, error) {
		existing, err := tx.Operation(ctx, cmd.BookID, cmd.OperationID)
		if err != nil {
			return Receipt{}, err
		}
		if existing != nil {
			if !receiptAccess(caller, existing.Receipt) {
				return Receipt{}, ErrPermission
			}
			if !bytes.Equal(existing.Canonical, canonical) {
				return Receipt{}, ErrConflict
			}
			return existing.Receipt, nil
		}
		now := s.now().UTC()
		state, err := tx.Load(ctx, cmd, now)
		if err != nil {
			return Receipt{}, err
		}
		if !state.Book.Synthetic || state.Book.Validate() != nil {
			return Receipt{}, ErrInvalid
		}
		base := Receipt{CallerIdentity: caller.Identity, SchemaVersion: "1", BookID: cmd.BookID, OperationID: cmd.OperationID, Kind: cmd.Kind,
			RequestHash: hash(canonical), Outcome: "APPLIED", PaymentID: cmd.PaymentID(), HoldID: cmd.HoldID(),
			RecordedAt: now, EventID: eventID, SubjectIDs: []domain.ID{}, ControlVersions: map[string]string{}, AccountVersions: map[domain.ID]string{}}
		for key, c := range state.Controls {
			base.ControlVersions[key.String()] = versionString(c.Version)
		}
		p := Plan{Command: cmd, Canonical: canonical, Receipt: base}
		reason, err := s.evaluate(caller, cmd, state, now, &p)
		if err != nil {
			return Receipt{}, err
		}
		if reason != "" {
			evidence, subjects := p.Evidence, p.Receipt.SubjectIDs
			p = Plan{Command: cmd, Canonical: canonical, Receipt: base, Evidence: evidence}
			p.Receipt.Outcome = "REJECTED"
			p.Receipt.Reason = reason
			p.Receipt.SubjectIDs = subjects
		}
		if cmd.Kind == TransferInternal || cmd.Kind == ReservePayout {
			if state.Claim == nil {
				claim := Claim{PaymentID: cmd.PaymentID(), AdmissionOperationID: cmd.OperationID, AdmissionOutcome: p.Receipt.Outcome,
					MaterialHash: hash(canonical), State: p.Receipt.Outcome, ResultOperationID: cmd.OperationID}
				if p.Receipt.Outcome == "APPLIED" && cmd.Kind == ReservePayout {
					claim.State = string(domain.Reserved)
					claim.HoldID = cmd.Reserve.HoldID
				}
				p.Claim = &Change[Claim]{After: claim}
			}
		}
		if p.Journal != nil {
			p.Receipt.JournalID = journalID
		}
		for _, b := range p.Balances {
			p.Receipt.AccountVersions[b.After.AccountID] = versionString(b.After.Version)
		}
		for _, c := range p.Controls {
			p.Receipt.ControlVersions[c.After.Key.String()] = versionString(c.After.Version)
		}
		if p.Hold != nil {
			p.Receipt.HoldVersion = p.Hold.After.State.Version
		}
		p.Receipt.LimitChange = p.Usage
		if state.Policy != nil {
			p.Receipt.PolicyID = state.Policy.ID
		} else if state.Hold != nil {
			p.Receipt.PolicyID = state.Hold.Terms.PolicyID
		}
		if p.Evidence != nil {
			p.Receipt.EvidenceID = p.Evidence.ID
		}
		sort.Slice(p.Receipt.SubjectIDs, func(i, j int) bool { return p.Receipt.SubjectIDs[i] < p.Receipt.SubjectIDs[j] })
		if err := tx.Save(ctx, p); err != nil {
			return Receipt{}, err
		}
		return p.Receipt, nil
	})
}
func (s *Service) GetOperation(ctx context.Context, c Caller, book, id domain.ID) (Receipt, error) {
	if !book.Valid() || !id.Valid() {
		return Receipt{}, ErrInvalid
	}
	if !c.Allows(book, "read") {
		return Receipt{}, ErrPermission
	}
	op, err := s.store.Operation(ctx, book, id)
	if err != nil {
		return Receipt{}, err
	}
	if op == nil {
		return Receipt{}, ErrNotFound
	}
	if !receiptAccess(c, op.Receipt) {
		return Receipt{}, ErrPermission
	}
	return op.Receipt, nil
}
func (s *Service) GetBalance(ctx context.Context, c Caller, book, id domain.ID) (domain.Account, Balance, error) {
	if !book.Valid() || !id.Valid() {
		return domain.Account{}, Balance{}, ErrInvalid
	}
	if !c.Allows(book, "read") {
		return domain.Account{}, Balance{}, ErrPermission
	}
	a, b, err := s.store.Balance(ctx, book, id)
	if err != nil {
		return a, b, err
	}
	if !c.AllowsSubject(a.Owner) {
		return domain.Account{}, Balance{}, ErrPermission
	}
	return a, b, nil
}
func (s *Service) Export(ctx context.Context, c Caller, book domain.ID) (Snapshot, error) {
	if !book.Valid() {
		return Snapshot{}, ErrInvalid
	}
	if !c.Allows(book, "verify") || len(c.Subjects) > 0 {
		return Snapshot{}, ErrPermission
	}
	return s.store.Snapshot(ctx, book)
}
func (s *Service) Ready(ctx context.Context) error { return s.store.Ready(ctx) }

func versionString(v int64) string { return strconv.FormatInt(v, 10) }
