package cockroach

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func readBook(ctx context.Context, q queryer, id domain.ID) (application.Book, error) {
	b := application.Book{Book: domain.Book{ID: id}}
	var cap string
	err := q.QueryRow(ctx, "SELECT currency,scale,synthetic,default_cap::STRING FROM books WHERE book_id=$1", string(id)).Scan(&b.Currency, &b.Scale, &b.Synthetic, &cap)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, application.ErrNotFound
	}
	if err != nil {
		return b, err
	}
	b.DefaultCap, err = domain.ParseWide(cap)
	if err != nil || b.Validate() != nil {
		return b, application.ErrIntegrity
	}
	return b, nil
}
func readAccount(ctx context.Context, q queryer, b domain.Book, id domain.ID) (*domain.Account, error) {
	var owner, purpose string
	err := q.QueryRow(ctx, "SELECT owner_id::STRING,purpose FROM accounts WHERE book_id=$1 AND account_id=$2", string(b.ID), string(id)).Scan(&owner, &purpose)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a := domain.Account{ID: id, Book: b, Owner: domain.ID(owner), Purpose: domain.Purpose(purpose)}
	if a.Validate() != nil {
		return nil, application.ErrIntegrity
	}
	return &a, nil
}
func readBalance(ctx context.Context, q queryer, book, id domain.ID, lock bool) (*application.Balance, error) {
	sql := "SELECT debits::STRING,credits::STRING,held::STRING,version FROM account_balances WHERE book_id=$1 AND account_id=$2"
	if lock {
		sql += " FOR UPDATE"
	}
	var d, c, h string
	b := application.Balance{AccountID: id}
	err := q.QueryRow(ctx, sql, string(book), string(id)).Scan(&d, &c, &h, &b.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	b.Totals.Debits, err = domain.ParseWide(d)
	if err != nil {
		return nil, application.ErrIntegrity
	}
	b.Totals.Credits, err = domain.ParseWide(c)
	if err != nil {
		return nil, application.ErrIntegrity
	}
	b.Totals.Held, err = domain.ParseWide(h)
	if err != nil {
		return nil, application.ErrIntegrity
	}
	return &b, nil
}
func readHold(ctx context.Context, q queryer, cmd application.Command, lock bool) (*application.Hold, error) {
	if cmd.HoldID() == "" {
		return nil, nil
	}
	sql := "SELECT hold_id::STRING,terms,expires_at,state,version FROM holds WHERE book_id=$1 AND hold_id=$2"
	args := []any{string(cmd.BookID), string(cmd.HoldID())}
	if cmd.Kind == application.ReservePayout && !lock {
		sql = "SELECT hold_id::STRING,terms,expires_at,state,version FROM holds WHERE book_id=$1 AND (hold_id=$2 OR attempt_id=$3) ORDER BY hold_id LIMIT 1"
		args = append(args, string(cmd.Reserve.AttemptID))
	}
	if lock {
		sql += " FOR UPDATE"
	}
	var id, state string
	var body []byte
	var expires time.Time
	var version int64
	err := q.QueryRow(ctx, sql, args...).Scan(&id, &body, &expires, &state, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var h application.Hold
	if json.Unmarshal(body, &h) != nil || h.ID != domain.ID(id) {
		return nil, application.ErrIntegrity
	}
	h.State = domain.HoldStateValue{State: domain.HoldState(state), Version: version, ExpiresAt: expires}
	return &h, nil
}
func (t *tx) Load(ctx context.Context, cmd application.Command, now time.Time) (application.State, error) {
	st := application.State{Accounts: map[domain.ID]domain.Account{}, Balances: map[domain.ID]application.Balance{}, Controls: map[application.ControlKey]application.Control{}}
	var err error
	st.Book, err = readBook(ctx, t, cmd.BookID)
	if err != nil {
		return st, err
	}
	// Claim is first in the universal contested-row order. A missing claim is
	// arbitrated by its unique key and SERIALIZABLE conflict detection.
	if payment := cmd.PaymentID(); payment != "" {
		var admission, outcome, digest, state, result string
		var hold *string
		err = t.QueryRow(ctx, "SELECT admission_operation_id::STRING,admission_outcome,material_hash,state,hold_id::STRING,result_operation_id::STRING FROM business_claims WHERE book_id=$1 AND payment_id=$2 AND stage='EXECUTION' FOR UPDATE", string(cmd.BookID), string(payment)).Scan(&admission, &outcome, &digest, &state, &hold, &result)
		if err == nil {
			claim := application.Claim{PaymentID: payment, AdmissionOperationID: domain.ID(admission), AdmissionOutcome: outcome, MaterialHash: digest, State: state, ResultOperationID: domain.ID(result)}
			if hold != nil {
				claim.HoldID = domain.ID(*hold)
			}
			st.Claim = &claim
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return st, err
		}
	}
	// Immutable terms are discovery data; current hold state is locked after controls.
	st.Hold, err = readHold(ctx, t, cmd, false)
	if err != nil {
		return st, err
	}
	accountIDs := map[domain.ID]bool{}
	controlKeys := map[application.ControlKey]bool{}
	var source domain.ID
	var policyID domain.ID
	switch cmd.Kind {
	case application.CreateAccount:
		accountIDs[cmd.Create.AccountID] = true
		controlKeys[application.ControlKey{Kind: "SUBJECT", ID: cmd.Create.OwnerID}] = true
		controlKeys[application.ControlKey{Kind: "ACCOUNT", ID: cmd.Create.AccountID}] = true
	case application.SetAccountControl:
		controlKeys[cmd.Control.Key] = true
		if cmd.Control.Key.Kind == "ACCOUNT" {
			accountIDs[cmd.Control.Key.ID] = true
		}
	case application.TransferInternal:
		source = cmd.Transfer.Terms.SourceID
		policyID = cmd.Transfer.Terms.PolicyID
		accountIDs[source] = true
		accountIDs[cmd.Transfer.Terms.BeneficiaryID] = true
	case application.ReservePayout:
		source = cmd.Reserve.Terms.SourceID
		policyID = cmd.Reserve.Terms.PolicyID
		accountIDs[source] = true
	default:
		if st.Hold != nil {
			source = st.Hold.Terms.SourceID
			accountIDs[source] = true
			accountIDs[st.Hold.PoolID] = true
			accountIDs[st.Hold.FeeAccountID] = true
		}
	}
	if policyID != "" {
		var fee int64
		var feeID, pool, provider, capability string
		var maxSeconds int64
		err = t.QueryRow(ctx, "SELECT fee,fee_account_id::STRING,pool_id::STRING,provider_account_id::STRING,capability_id::STRING,max_hold_seconds FROM posting_policies WHERE book_id=$1 AND policy_id=$2", string(cmd.BookID), string(policyID)).Scan(&fee, &feeID, &pool, &provider, &capability, &maxSeconds)
		if err == nil {
			amount, e := domain.NewAmount(fee)
			if e != nil {
				return st, application.ErrIntegrity
			}
			st.Policy = &application.Policy{ID: policyID, Fee: amount, FeeAccountID: domain.ID(feeID), PoolID: domain.ID(pool), ProviderAccountID: domain.ID(provider), CapabilityID: domain.ID(capability), MaxHoldSeconds: maxSeconds}
			accountIDs[domain.ID(feeID)] = true
			if cmd.Kind == application.ReservePayout {
				accountIDs[domain.ID(pool)] = true
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return st, err
		}
	}
	ids := make([]domain.ID, 0, len(accountIDs))
	for id := range accountIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		a, e := readAccount(ctx, t, st.Book.Book, id)
		if e != nil {
			return st, e
		}
		if a == nil {
			continue
		}
		st.Accounts[id] = *a
		if cmd.Kind != application.SetAccountControl && a.Purpose != domain.FeeIncome {
			controlKeys[application.ControlKey{Kind: "SUBJECT", ID: a.Owner}] = true
			controlKeys[application.ControlKey{Kind: "ACCOUNT", ID: a.ID}] = true
		}
	}
	keys := make([]application.ControlKey, 0, len(controlKeys))
	for k := range controlKeys {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Kind != keys[j].Kind {
			return keys[i].Kind == "SUBJECT"
		}
		return keys[i].ID < keys[j].ID
	})
	for _, k := range keys {
		c := application.Control{Key: k}
		var cap string
		err = t.QueryRow(ctx, "SELECT version,epoch,debit_frozen,credit_frozen,daily_cap::STRING FROM spending_controls WHERE book_id=$1 AND subject_kind=$2 AND subject_id=$3 FOR UPDATE", string(cmd.BookID), k.Kind, string(k.ID)).Scan(&c.Version, &c.Epoch, &c.DebitFrozen, &c.CreditFrozen, &cap)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return st, err
		}
		c.DailyCap, err = domain.ParseWide(cap)
		if err != nil {
			return st, application.ErrIntegrity
		}
		st.Controls[k] = c
	}
	if st.Hold != nil && cmd.Kind != application.ReservePayout {
		st.Hold, err = readHold(ctx, t, cmd, true)
		if err != nil {
			return st, err
		}
	}
	if a, ok := st.Accounts[source]; ok && cmd.Kind != application.MarkHoldExposed {
		bucket := application.Bucket(now)
		if cmd.Kind != application.TransferInternal && cmd.Kind != application.ReservePayout && st.Hold != nil {
			bucket = st.Hold.Bucket
		}
		u := application.Usage{OwnerID: a.Owner, Bucket: bucket}
		var reserved, consumed string
		err = t.QueryRow(ctx, "SELECT reserved::STRING,consumed::STRING,version FROM limit_usage WHERE book_id=$1 AND owner_id=$2 AND bucket=$3::DATE FOR UPDATE", string(cmd.BookID), string(a.Owner), bucket).Scan(&reserved, &consumed, &u.Version)
		if err == nil {
			u.Values.Reserved, err = domain.ParseWide(reserved)
			if err != nil {
				return st, application.ErrIntegrity
			}
			u.Values.Consumed, err = domain.ParseWide(consumed)
			if err != nil {
				return st, application.ErrIntegrity
			}
			st.Usage = &u
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return st, err
		}
	}
	for _, id := range ids {
		b, e := readBalance(ctx, t, cmd.BookID, id, true)
		if e != nil {
			return st, e
		}
		if b != nil {
			st.Balances[id] = *b
		}
	}
	return st, nil
}
func (s *Store) Balance(ctx context.Context, book, id domain.ID) (domain.Account, application.Balance, error) {
	raw, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Account{}, application.Balance{}, application.ErrUnavailable
	}
	defer raw.Rollback(context.Background())
	b, err := readBook(ctx, raw, book)
	if err != nil {
		return domain.Account{}, application.Balance{}, err
	}
	a, err := readAccount(ctx, raw, b.Book, id)
	if err != nil {
		return domain.Account{}, application.Balance{}, application.ErrUnavailable
	}
	if a == nil {
		return domain.Account{}, application.Balance{}, application.ErrNotFound
	}
	balance, err := readBalance(ctx, raw, book, id, false)
	if err != nil {
		return domain.Account{}, application.Balance{}, application.ErrUnavailable
	}
	if balance == nil {
		return domain.Account{}, application.Balance{}, application.ErrIntegrity
	}
	if err = raw.Commit(ctx); err != nil {
		return domain.Account{}, application.Balance{}, application.ErrUnavailable
	}
	return *a, *balance, nil
}
