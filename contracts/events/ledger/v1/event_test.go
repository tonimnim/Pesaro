package ledgerevent

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func sample(t *testing.T) Event {
	t.Helper()
	data := []byte(`{"schema_version":"1","book_id":"00000000-0000-4000-8000-000000000001","event_id":"00000000-0000-4000-8000-000000000002","operation_id":"00000000-0000-4000-8000-000000000003","request_hash":"` + strings.Repeat("a", 64) + `","recorded_at":"2026-09-25T10:00:00Z","kind":"TRANSFER_INTERNAL","outcome":"APPLIED","future_receipt_field":"preserved"}`)
	event, err := New("00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003", data)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestCanonicalEnvelopeAndBindings(t *testing.T) {
	event := sample(t)
	canonical, err := event.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, canonical, "", "  ") != nil {
		t.Fatal("indent")
	}
	_, other, err := Decode(pretty.Bytes())
	if err != nil || !bytes.Equal(canonical, other) {
		t.Fatal("formatting changed event identity", err)
	}
	for name, mutate := range map[string]func(*Event){
		"book":          func(e *Event) { e.BookID = e.OperationID },
		"event":         func(e *Event) { e.EventID = e.OperationID },
		"operation":     func(e *Event) { e.OperationID = e.EventID },
		"country":       func(e *Event) { e.Country = "UG" },
		"schema":        func(e *Event) { e.Version = "2" },
		"producer":      func(e *Event) { e.Producer = "payments" },
		"time":          func(e *Event) { e.OccurredAt = "2026-09-26T10:00:00Z" },
		"empty receipt": func(e *Event) { e.Receipt = []byte(`{}`) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := event
			mutate(&changed)
			if _, err := changed.Encode(); err == nil {
				t.Fatal("unbound event accepted")
			}
		})
	}
	for _, invalid := range [][]byte{
		append(canonical, []byte(`{}`)...),
		[]byte(strings.Replace(string(canonical), `"producer":"ledger"`, `"producer":"ledger","producer":"ledger"`, 1)),
		append([]byte(`{"extra":true,`), canonical[1:]...),
		bytes.Repeat([]byte(" "), MaxBytes+1),
	} {
		if _, _, err := Decode(invalid); err == nil {
			t.Fatal("malformed/ambiguous envelope accepted")
		}
	}
}
