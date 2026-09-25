// Package application implements typed Ledger commands. It has no SQL or RPC imports.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

type Kind string

const (
	CreateAccount     Kind = "CREATE_ACCOUNT"
	TransferInternal  Kind = "TRANSFER_INTERNAL"
	ReservePayout     Kind = "RESERVE_PAYOUT"
	MarkHoldExposed   Kind = "MARK_HOLD_EXPOSED"
	CapturePayout     Kind = "CAPTURE_PAYOUT"
	ReleasePayout     Kind = "RELEASE_PAYOUT"
	SetAccountControl Kind = "SET_ACCOUNT_CONTROL"
)

var (
	ErrInvalid     = errors.New("invalid command")
	ErrPermission  = errors.New("permission denied")
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("idempotency conflict")
	ErrUnavailable = errors.New("ledger authority unavailable")
	ErrUnknown     = errors.New("financial outcome unknown; recover original operation")
	ErrIntegrity   = errors.New("ledger integrity check failed")
)

type Caller struct {
	Identity    string
	Books       map[domain.ID]bool
	Permissions map[string]bool
	Subjects    map[domain.ID]bool // Empty grants all subjects within explicitly allowed books.
}

func (c Caller) Allows(book domain.ID, permission string) bool {
	return c.Identity != "" && c.Books[book] && c.Permissions[permission]
}
func (c Caller) AllowsSubject(subject domain.ID) bool {
	return len(c.Subjects) == 0 || c.Subjects[subject]
}

type Command struct {
	BookID      domain.ID
	OperationID domain.ID
	Kind        Kind
	Create      *Create
	Transfer    *Transfer
	Reserve     *Reserve
	Expose      *Expose
	Capture     *Capture
	Release     *Release
	Control     *SetControl
}
type Create struct {
	AccountID domain.ID      `json:"account_id"`
	OwnerID   domain.ID      `json:"owner_id"`
	Purpose   domain.Purpose `json:"purpose"`
}
type Terms struct {
	PaymentID     domain.ID     `json:"payment_id"`
	SourceID      domain.ID     `json:"source_id"`
	BeneficiaryID domain.ID     `json:"beneficiary_id"`
	Principal     domain.Amount `json:"principal"`
	Fee           domain.Amount `json:"fee"`
	Currency      string        `json:"currency"`
	PolicyID      domain.ID     `json:"policy_id"`
	QuoteID       domain.ID     `json:"quote_id"`
	Grant         SignedGrant   `json:"grant"`
}
type Transfer struct {
	Terms Terms `json:"terms"`
}
type Reserve struct {
	Terms     Terms     `json:"terms"`
	HoldID    domain.ID `json:"hold_id"`
	AttemptID domain.ID `json:"attempt_id"`
	PoolID    domain.ID `json:"pool_id"`
	ExpiresAt time.Time `json:"expires_at"`
}
type HoldRef struct {
	PaymentID       domain.ID `json:"payment_id"`
	HoldID          domain.ID `json:"hold_id"`
	AttemptID       domain.ID `json:"attempt_id"`
	ExpectedVersion int64     `json:"expected_version,string"`
}
type Expose struct {
	Ref     HoldRef   `json:"ref"`
	GrantID domain.ID `json:"grant_id"`
}
type Capture struct {
	Ref      HoldRef        `json:"ref"`
	Evidence SignedEvidence `json:"evidence"`
}
type Release struct {
	Ref      HoldRef         `json:"ref"`
	Reason   string          `json:"reason"` // CANCEL, EXPIRY or FINAL_FAILURE
	Evidence *SignedEvidence `json:"evidence"`
}
type ControlKey struct {
	Kind string    `json:"kind"` // SUBJECT or ACCOUNT
	ID   domain.ID `json:"id"`
}

func (k ControlKey) String() string { return k.Kind + ":" + string(k.ID) }

type SetControl struct {
	Key             ControlKey  `json:"key"`
	ExpectedVersion int64       `json:"expected_version,string"`
	DebitFrozen     bool        `json:"debit_frozen"`
	CreditFrozen    bool        `json:"credit_frozen"`
	DailyCap        domain.Wide `json:"daily_cap"`
	RevokeGrants    bool        `json:"revoke_grants"`
	Reason          string      `json:"reason"`
}

type Book struct {
	domain.Book
	DefaultCap domain.Wide
	Synthetic  bool
}
type Policy struct {
	ID                domain.ID
	Fee               domain.Amount
	FeeAccountID      domain.ID
	PoolID            domain.ID
	ProviderAccountID domain.ID
	CapabilityID      domain.ID
	MaxHoldSeconds    int64
}
type Balance struct {
	AccountID domain.ID     `json:"account_id"`
	Totals    domain.Totals `json:"totals"`
	Version   int64         `json:"version,string"`
}
type Control struct {
	Key          ControlKey  `json:"key"`
	Version      int64       `json:"version,string"`
	Epoch        int64       `json:"epoch,string"`
	DebitFrozen  bool        `json:"debit_frozen"`
	CreditFrozen bool        `json:"credit_frozen"`
	DailyCap     domain.Wide `json:"daily_cap"`
}
type Hold struct {
	ID                domain.ID             `json:"id"`
	Terms             Terms                 `json:"terms"`
	OwnerID           domain.ID             `json:"owner_id"`
	AttemptID         domain.ID             `json:"attempt_id"`
	PoolID            domain.ID             `json:"pool_id"`
	FeeAccountID      domain.ID             `json:"fee_account_id"`
	ProviderAccountID domain.ID             `json:"provider_account_id"`
	CapabilityID      domain.ID             `json:"capability_id"`
	Bucket            string                `json:"bucket"`
	State             domain.HoldStateValue `json:"state"`
}
type Claim struct {
	PaymentID            domain.ID `json:"payment_id"`
	AdmissionOperationID domain.ID `json:"admission_operation_id"`
	AdmissionOutcome     string    `json:"admission_outcome"`
	MaterialHash         string    `json:"material_hash"`
	State                string    `json:"state"`
	HoldID               domain.ID `json:"hold_id"`
	ResultOperationID    domain.ID `json:"result_operation_id"`
}
type Usage struct {
	OwnerID domain.ID    `json:"owner_id"`
	Bucket  string       `json:"bucket"`
	Values  domain.Usage `json:"values"`
	Version int64        `json:"version,string"`
}
type Receipt struct {
	CallerIdentity  string               `json:"caller_identity"`
	SchemaVersion   string               `json:"schema_version"`
	BookID          domain.ID            `json:"book_id"`
	OperationID     domain.ID            `json:"operation_id"`
	Kind            Kind                 `json:"kind"`
	RequestHash     string               `json:"request_hash"`
	Outcome         string               `json:"outcome"`
	Reason          string               `json:"reason"`
	PaymentID       domain.ID            `json:"payment_id"`
	SubjectIDs      []domain.ID          `json:"subject_ids"`
	JournalID       domain.ID            `json:"journal_id"`
	HoldID          domain.ID            `json:"hold_id"`
	HoldVersion     int64                `json:"hold_version,string"`
	ControlVersions map[string]string    `json:"control_versions"`
	AccountVersions map[domain.ID]string `json:"account_versions"`
	PolicyID        domain.ID            `json:"policy_id"`
	EvidenceID      domain.ID            `json:"evidence_id"`
	LimitChange     *Change[Usage]       `json:"limit_change"`
	RecordedAt      time.Time            `json:"recorded_at"`
	EventID         domain.ID            `json:"event_id"`
}
type Operation struct {
	Canonical []byte
	Receipt   Receipt
}
type State struct {
	Book     Book
	Accounts map[domain.ID]domain.Account
	Balances map[domain.ID]Balance
	Controls map[ControlKey]Control
	Policy   *Policy
	Claim    *Claim
	Hold     *Hold
	Usage    *Usage
}

type Change[T any] struct {
	Before *T
	After  T
}
type Plan struct {
	Command    Command
	Canonical  []byte
	Receipt    Receipt
	NewAccount *domain.Account
	Journal    *domain.Journal
	Balances   []Change[Balance]
	Controls   []Change[Control]
	Hold       *Change[Hold]
	Usage      *Change[Usage]
	Claim      *Change[Claim]
	Evidence   *Evidence
}

type Transaction interface {
	Operation(context.Context, domain.ID, domain.ID) (*Operation, error)
	Load(context.Context, Command, time.Time) (State, error)
	Save(context.Context, Plan) error
}
type Store interface {
	Transact(context.Context, func(Transaction) (Receipt, error)) (Receipt, error)
	Operation(context.Context, domain.ID, domain.ID) (*Operation, error)
	Balance(context.Context, domain.ID, domain.ID) (domain.Account, Balance, error)
	Snapshot(context.Context, domain.ID) (Snapshot, error)
	Ready(context.Context) error
}

type Snapshot struct {
	SchemaVersion string                      `json:"schema_version"`
	Cut           string                      `json:"cut"`
	BookID        domain.ID                   `json:"book_id"`
	Tables        map[string][]map[string]any `json:"tables"`
	Digests       map[string]string           `json:"digests"`
	Counts        map[string]int              `json:"counts"`
}
