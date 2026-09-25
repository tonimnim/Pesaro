package inbox

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
)

func testID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func testEvent(t *testing.T) ledgerevent.Event {
	t.Helper()
	r := ledgerevent.ReceiptIdentity{Version: "1", BookID: testID(), EventID: testID(), OperationID: testID(), RequestHash: strings.Repeat("a", 64), Kind: "TRANSFER_INTERNAL", Outcome: "APPLIED", RecordedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	e, err := ledgerevent.New(r.BookID, r.EventID, r.OperationID, data)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("PESAR_RECONCILIATION_TEST_RUNTIME_URL")
	if dsn == "" {
		t.Skip("requires prepared real Reconciliation database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, dsn, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Ready(ctx); err != nil {
		t.Fatal("prepare schema once before tests", err)
	}
	return s
}

func counts(t *testing.T, s *Store, book string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var inbox, projection int
	err := s.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM event_inbox WHERE book_id=$1),(SELECT count(*) FROM ledger_operations WHERE book_id=$1)", book).Scan(&inbox, &projection)
	if err != nil || inbox != want || projection != want {
		t.Fatalf("inbox=%d projection=%d want=%d: %v", inbox, projection, want, err)
	}
}

func TestConcurrentReplayAndPermanentEventBinding(t *testing.T) {
	s := testStore(t)
	e := testEvent(t)
	data, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 12)
	for range 12 {
		go func() { <-start; done <- s.Accept(ctx, data) }()
	}
	close(start)
	for range 12 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	counts(t, s, e.BookID, 1)
	for _, mode := range []string{"changed_payload", "new_event_same_operation", "same_event_new_operation"} {
		r, err := e.Identity()
		if err != nil {
			t.Fatal(err)
		}
		switch mode {
		case "changed_payload":
			r.Outcome = "REJECTED"
		case "new_event_same_operation":
			r.EventID = testID()
		case "same_event_new_operation":
			r.OperationID = testID()
		}
		body, _ := json.Marshal(r)
		changed, err := ledgerevent.New(r.BookID, r.EventID, r.OperationID, body)
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := changed.Encode()
		if err = s.Accept(ctx, wire); !errors.Is(err, ErrConflict) {
			t.Fatal(mode, err)
		}
	}
	counts(t, s, e.BookID, 1)
	// A fresh connection and non-canonical whitespace still recover one record.
	restarted, err := Open(ctx, os.Getenv("PESAR_RECONCILIATION_TEST_RUNTIME_URL"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	pretty, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Accept(ctx, pretty); err != nil {
		t.Fatal(err)
	}
	counts(t, restarted, e.BookID, 1)
}

func TestInboxRuntimePrivileges(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, query := range []string{
		"UPDATE event_inbox SET digest=digest WHERE false",
		"DELETE FROM ledger_operations WHERE false",
		"SELECT * FROM pesaro_ledger.public.account_balances LIMIT 0",
		"CREATE TABLE forbidden_inbox_probe(id INT)",
	} {
		if _, err := s.pool.Exec(ctx, query); code(err) != "42501" {
			t.Fatalf("runtime authority exceeded (%s): %v", query, err)
		}
	}
}
