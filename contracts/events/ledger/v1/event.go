// Package ledgerevent defines the immutable LedgerOperationResolved v1 wire
// envelope. It has no Ledger domain, persistence or posting dependencies.
package ledgerevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

const Path = "/internal/events/ledger/v1"
const MaxBytes = 64 << 10

var ErrInvalid = errors.New("invalid Ledger event")
var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var hash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidID(s string) bool { return uuid.MatchString(s) }

type Event struct {
	Version     string          `json:"schema_version"`
	Producer    string          `json:"producer"`
	Type        string          `json:"event_type"`
	Country     string          `json:"country_id"`
	BookID      string          `json:"book_id"`
	EventID     string          `json:"event_id"`
	OperationID string          `json:"operation_id"`
	OccurredAt  string          `json:"occurred_at"`
	Receipt     json.RawMessage `json:"receipt"`
}

// ReceiptIdentity projects only immutable identity/classification fields. The
// complete receipt is retained as evidence, never used to authorize spending.
type ReceiptIdentity struct {
	Version     string `json:"schema_version"`
	BookID      string `json:"book_id"`
	EventID     string `json:"event_id"`
	OperationID string `json:"operation_id"`
	RequestHash string `json:"request_hash"`
	Kind        string `json:"kind"`
	Outcome     string `json:"outcome"`
	RecordedAt  string `json:"recorded_at"`
}

func (e Event) Identity() (ReceiptIdentity, error) {
	var r ReceiptIdentity
	err := json.Unmarshal(e.Receipt, &r)
	return r, err
}

func New(book, event, operation string, receipt []byte) (Event, error) {
	e := Event{Version: "1", Producer: "ledger", Type: "LedgerOperationResolved", Country: "KE", BookID: book, EventID: event, OperationID: operation, Receipt: receipt}
	r, err := e.Identity()
	if err != nil {
		return Event{}, ErrInvalid
	}
	e.OccurredAt = r.RecordedAt
	_, err = e.Encode()
	return e, err
}

func (e Event) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, ErrInvalid
	}
	_, canonical, err := Decode(data)
	return canonical, err
}

func Decode(data []byte) (Event, []byte, error) {
	if len(data) == 0 || len(data) > MaxBytes {
		return Event{}, nil, ErrInvalid
	}
	// JCS also rejects duplicate object keys; decoding first would hide them.
	canonical, err := jsoncanonicalizer.Transform(data)
	if err != nil {
		return Event{}, nil, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	var e Event
	var extra any
	if decoder.Decode(&e) != nil || decoder.Decode(&extra) != io.EOF {
		return Event{}, nil, ErrInvalid
	}
	r, err := e.Identity()
	when, timeErr := time.Parse(time.RFC3339Nano, e.OccurredAt)
	if err != nil || timeErr != nil || when.IsZero() || e.Version != "1" || e.Producer != "ledger" || e.Type != "LedgerOperationResolved" || e.Country != "KE" ||
		!ValidID(e.BookID) || !ValidID(e.EventID) || !ValidID(e.OperationID) || r.Version != "1" || r.BookID != e.BookID || r.EventID != e.EventID || r.OperationID != e.OperationID || r.RecordedAt != e.OccurredAt || !hash.MatchString(r.RequestHash) || (r.Outcome != "APPLIED" && r.Outcome != "REJECTED") {
		return Event{}, nil, ErrInvalid
	}
	switch r.Kind {
	case "FIXTURE_FUND", "CREATE_ACCOUNT", "TRANSFER_INTERNAL", "RESERVE_PAYOUT", "MARK_HOLD_EXPOSED", "CAPTURE_PAYOUT", "RELEASE_PAYOUT", "SET_ACCOUNT_CONTROL":
	default:
		return Event{}, nil, ErrInvalid
	}
	return e, canonical, nil
}
