package inbox

import (
	"context"
	"os"
	"testing"
	"time"

	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
)

// The evidence scripts kill and restart the actual shared local engine between
// these two separate test invocations. There is no mocked persistence fallback.
func TestInboxRestartPrepare(t *testing.T) {
	path := os.Getenv("PESAR_LEDGER_RESTART_PREPARE")
	if path == "" {
		t.Skip("requires database restart orchestration")
	}
	s := testStore(t)
	event := testEvent(t)
	data, err := event.Encode()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = s.Accept(ctx, data); err != nil {
		t.Fatal(err)
	}
	counts(t, s, event.BookID, 1)
	if err = os.WriteFile(path+".inbox.json", data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestInboxRestartRecover(t *testing.T) {
	path := os.Getenv("PESAR_LEDGER_RESTART_RECOVER")
	if path == "" {
		t.Skip("requires database restart orchestration")
	}
	data, err := os.ReadFile(path + ".inbox.json")
	if err != nil {
		t.Fatal(err)
	}
	event, _, err := ledgerevent.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	s := testStore(t)
	counts(t, s, event.BookID, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 2 {
		if err = s.Accept(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	counts(t, s, event.BookID, 1)
	t.Log("inbox and projection survived actual engine restart; original event replay changed neither count")
}
