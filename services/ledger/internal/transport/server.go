// Package transport exposes Ledger's versioned, authenticated gRPC boundary.
package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"

	ledgerv1 "github.com/tonimnim/Pesaro/contracts/gen/ledger/v1"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	ledgerv1.UnimplementedLedgerServer
	service *application.Service
}

func New(service *application.Service) *Server { return &Server{service: service} }

type callerKey struct{}

func WithCaller(ctx context.Context, caller application.Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}
func caller(ctx context.Context) application.Caller {
	c, _ := ctx.Value(callerKey{}).(application.Caller)
	return c
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	code, reason := codes.Unavailable, "UNAVAILABLE"
	switch {
	case errors.Is(err, application.ErrInvalid):
		code, reason = codes.InvalidArgument, "INVALID_ARGUMENT"
	case errors.Is(err, application.ErrPermission):
		code, reason = codes.PermissionDenied, "PERMISSION_DENIED"
	case errors.Is(err, application.ErrNotFound):
		code, reason = codes.NotFound, "NOT_FOUND"
	case errors.Is(err, application.ErrConflict):
		code, reason = codes.AlreadyExists, "IDEMPOTENCY_CONFLICT"
	case errors.Is(err, application.ErrIntegrity):
		code, reason = codes.Internal, "INTEGRITY_ERROR"
	case errors.Is(err, application.ErrUnknown):
		code, reason = codes.Unavailable, "OUTCOME_UNKNOWN"
	}
	st := status.New(code, reason)
	with, detailErr := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "ledger.pesaro"})
	if detailErr == nil {
		return with.Err()
	}
	return st.Err()
}
func decode(message proto.Message, out any) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		return application.ErrInvalid
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)
	if err != nil || len(data) > 16384 {
		return application.ErrInvalid
	}
	if err = json.Unmarshal(data, out); err != nil {
		return application.ErrInvalid
	}
	return nil
}
func command(e *ledgerv1.Envelope, kind application.Kind) (application.Command, error) {
	if e == nil || e.SchemaVersion != "1" {
		return application.Command{}, application.ErrInvalid
	}
	book, err := domain.ParseID(e.BookId)
	if err != nil {
		return application.Command{}, application.ErrInvalid
	}
	op, err := domain.ParseID(e.OperationId)
	if err != nil {
		return application.Command{}, application.ErrInvalid
	}
	return application.Command{BookID: book, OperationID: op, Kind: kind}, nil
}
func (s *Server) execute(ctx context.Context, c application.Command) (*ledgerv1.Receipt, error) {
	r, err := s.service.Execute(ctx, caller(ctx), c)
	if err != nil {
		return nil, rpcError(err)
	}
	return receipt(r), nil
}
func (s *Server) CreateAccount(ctx context.Context, r *ledgerv1.CreateAccountRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.CreateAccount)
	if err != nil {
		return nil, rpcError(err)
	}
	account, err := domain.ParseID(r.GetAccountId())
	if err != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	owner, err := domain.ParseID(r.GetOwnerId())
	if err != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	c.Create = &application.Create{AccountID: account, OwnerID: owner, Purpose: domain.Purpose(r.GetPurpose())}
	return s.execute(ctx, c)
}
func (s *Server) TransferInternal(ctx context.Context, r *ledgerv1.TransferInternalRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.TransferInternal)
	if err != nil {
		return nil, rpcError(err)
	}
	c.Transfer = &application.Transfer{}
	if err = decode(r.GetTerms(), &c.Transfer.Terms); err != nil {
		return nil, rpcError(err)
	}
	return s.execute(ctx, c)
}
func (s *Server) ReservePayout(ctx context.Context, r *ledgerv1.ReservePayoutRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.ReservePayout)
	if err != nil {
		return nil, rpcError(err)
	}
	c.Reserve = &application.Reserve{}
	// Envelope is intentionally not part of the command-specific body.
	var body struct {
		Terms     application.Terms `json:"terms"`
		HoldID    domain.ID         `json:"hold_id"`
		AttemptID domain.ID         `json:"attempt_id"`
		PoolID    domain.ID         `json:"pool_id"`
		ExpiresAt json.RawMessage   `json:"expires_at"`
	}
	if err = decode(r, &body); err != nil {
		return nil, rpcError(err)
	}
	c.Reserve.Terms = body.Terms
	c.Reserve.HoldID = body.HoldID
	c.Reserve.AttemptID = body.AttemptID
	c.Reserve.PoolID = body.PoolID
	if err = json.Unmarshal(body.ExpiresAt, &c.Reserve.ExpiresAt); err != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	c.Reserve.ExpiresAt = c.Reserve.ExpiresAt.UTC()
	return s.execute(ctx, c)
}
func (s *Server) MarkHoldExposed(ctx context.Context, r *ledgerv1.MarkHoldExposedRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.MarkHoldExposed)
	if err != nil {
		return nil, rpcError(err)
	}
	grant, err := domain.ParseID(r.GetGrantId())
	if err != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	c.Expose = &application.Expose{GrantID: grant}
	if err = decode(r.GetRef(), &c.Expose.Ref); err != nil {
		return nil, rpcError(err)
	}
	return s.execute(ctx, c)
}
func (s *Server) CapturePayout(ctx context.Context, r *ledgerv1.CapturePayoutRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.CapturePayout)
	if err != nil {
		return nil, rpcError(err)
	}
	c.Capture = &application.Capture{}
	if err = decode(r.GetRef(), &c.Capture.Ref); err != nil {
		return nil, rpcError(err)
	}
	if err = decode(r.GetEvidence(), &c.Capture.Evidence); err != nil {
		return nil, rpcError(err)
	}
	return s.execute(ctx, c)
}
func (s *Server) ReleasePayout(ctx context.Context, r *ledgerv1.ReleasePayoutRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.ReleasePayout)
	if err != nil {
		return nil, rpcError(err)
	}
	c.Release = &application.Release{Reason: r.GetReason()}
	if err = decode(r.GetRef(), &c.Release.Ref); err != nil {
		return nil, rpcError(err)
	}
	if r.GetEvidence() != nil {
		c.Release.Evidence = &application.SignedEvidence{}
		if err = decode(r.Evidence, c.Release.Evidence); err != nil {
			return nil, rpcError(err)
		}
	}
	return s.execute(ctx, c)
}
func (s *Server) SetAccountControl(ctx context.Context, r *ledgerv1.SetAccountControlRequest) (*ledgerv1.Receipt, error) {
	c, err := command(r.GetEnvelope(), application.SetAccountControl)
	if err != nil {
		return nil, rpcError(err)
	}
	c.Control = &application.SetControl{}
	if err = decode(r, c.Control); err != nil {
		return nil, rpcError(err)
	}
	return s.execute(ctx, c)
}
func (s *Server) GetOperation(ctx context.Context, r *ledgerv1.GetOperationRequest) (*ledgerv1.Receipt, error) {
	book, e := domain.ParseID(r.GetBookId())
	if e != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	id, e := domain.ParseID(r.GetOperationId())
	if e != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	result, err := s.service.GetOperation(ctx, caller(ctx), book, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return receipt(result), nil
}
func (s *Server) GetBalance(ctx context.Context, r *ledgerv1.GetBalanceRequest) (*ledgerv1.Balance, error) {
	book, e := domain.ParseID(r.GetBookId())
	if e != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	id, e := domain.ParseID(r.GetAccountId())
	if e != nil {
		return nil, rpcError(application.ErrInvalid)
	}
	a, b, err := s.service.GetBalance(ctx, caller(ctx), book, id)
	if err != nil {
		return nil, rpcError(err)
	}
	posted, err := b.Totals.Posted(a.Purpose)
	if err != nil {
		return nil, rpcError(application.ErrIntegrity)
	}
	available, err := b.Totals.Available(a.Purpose)
	if err != nil {
		return nil, rpcError(application.ErrIntegrity)
	}
	return &ledgerv1.Balance{BookId: string(book), AccountId: string(id), Currency: a.Book.Currency, Scale: strconv.Itoa(a.Book.Scale), Purpose: string(a.Purpose), DebitsPosted: b.Totals.Debits.String(), CreditsPosted: b.Totals.Credits.String(), Held: b.Totals.Held.String(), Posted: posted.String(), Available: available.String(), Version: strconv.FormatInt(b.Version, 10)}, nil
}
func usage(u *application.Usage) *ledgerv1.Usage {
	if u == nil {
		return nil
	}
	return &ledgerv1.Usage{OwnerId: string(u.OwnerID), Bucket: u.Bucket, Reserved: u.Values.Reserved.String(), Consumed: u.Values.Consumed.String(), Version: strconv.FormatInt(u.Version, 10)}
}
func receipt(r application.Receipt) *ledgerv1.Receipt {
	out := &ledgerv1.Receipt{SchemaVersion: r.SchemaVersion, BookId: string(r.BookID), OperationId: string(r.OperationID), Kind: string(r.Kind), RequestHash: r.RequestHash, Outcome: r.Outcome, Reason: r.Reason, PaymentId: string(r.PaymentID), JournalId: string(r.JournalID), HoldId: string(r.HoldID), HoldVersion: strconv.FormatInt(r.HoldVersion, 10), ControlVersions: r.ControlVersions, AccountVersions: map[string]string{}, PolicyId: string(r.PolicyID), EvidenceId: string(r.EvidenceID), RecordedAt: r.RecordedAt.Format("2006-01-02T15:04:05.999999999Z07:00"), EventId: string(r.EventID), CallerIdentity: r.CallerIdentity}
	for _, id := range r.SubjectIDs {
		out.SubjectIds = append(out.SubjectIds, string(id))
	}
	for id, v := range r.AccountVersions {
		out.AccountVersions[string(id)] = v
	}
	if r.LimitChange != nil {
		out.LimitChange = &ledgerv1.LimitChange{Before: usage(r.LimitChange.Before), After: usage(&r.LimitChange.After)}
	}
	return out
}
func (s *Server) ExportLedgerSnapshot(r *ledgerv1.ExportLedgerSnapshotRequest, stream ledgerv1.Ledger_ExportLedgerSnapshotServer) error {
	book, err := domain.ParseID(r.GetBookId())
	if err != nil {
		return rpcError(application.ErrInvalid)
	}
	snapshot, err := s.service.Export(stream.Context(), caller(stream.Context()), book)
	if err != nil {
		return rpcError(err)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return rpcError(application.ErrIntegrity)
	}
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	const size = 64 << 10
	for offset := 0; offset < len(data); offset += size {
		end := min(offset+size, len(data))
		if err := stream.Send(&ledgerv1.SnapshotChunk{SchemaVersion: "1", BookId: string(book), Cut: snapshot.Cut, Index: uint32(offset / size), Data: data[offset:end], Sha256: digest, Last: end == len(data)}); err != nil {
			return err
		}
	}
	return nil
}
