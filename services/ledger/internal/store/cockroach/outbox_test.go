package cockroach_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
)

func TestOutboxLeaseAckLossAndLateEvent(t *testing.T) {
	e := setup(t)
	f := e.fixture
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := e.store.ClaimOutbox(ctx, f.BookID, 1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatal("initial fixture event missing", err)
	}
	other, err := e.store.ClaimOutbox(ctx, f.BookID, 1, time.Minute)
	if err != nil || len(other) != 0 {
		t.Fatal("concurrent lease duplicated", err)
	}
	// Simulate a crash after delivery but before acknowledgement by advancing
	// only delivery-lease expiry with separate administrative test credentials.
	admin, err := pgx.Connect(ctx, os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	if _, err = admin.Exec(ctx, "UPDATE outbox_delivery SET lease_until=now()-INTERVAL '1 second' WHERE book_id=$1", string(f.BookID)); err != nil {
		t.Fatal(err)
	}
	replay, err := e.store.ClaimOutbox(ctx, f.BookID, 1, time.Minute)
	if err != nil || len(replay) != 1 || replay[0].EventID != first[0].EventID || replay[0].LeaseToken == first[0].LeaseToken {
		t.Fatal("ack loss did not retain identity", err)
	}
	if err = e.store.AcknowledgeOutbox(ctx, first[0]); err != application.ErrConflict {
		t.Fatal("stale publisher acknowledged successor lease", err)
	}
	if err = e.store.AcknowledgeOutbox(ctx, replay[0]); err != nil {
		t.Fatal(err)
	}
	// Commit a later operation with an older recorded_at. Identity-based pending
	// selection must still find it after acknowledging the preceding event.
	old := time.Now().UTC().Add(-time.Hour)
	service := application.New(e.store, application.Trust{}, func() time.Time { return old })
	receipt, err := service.Execute(ctx, e.caller, application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.CreateAccount, Create: &application.Create{AccountID: domain.NewID(), OwnerID: domain.NewID(), Purpose: domain.Wallet}})
	if err != nil || receipt.Outcome != "APPLIED" {
		t.Fatal(receipt, err)
	}
	seen := map[domain.ID]bool{}
	n, err := e.store.PublishOutbox(ctx, f.BookID, 10, func(_ context.Context, event cockroach.LeasedFact) error { seen[event.EventID] = true; return nil })
	if err != nil || n != 1 || !seen[receipt.EventID] {
		t.Fatal("later commit skipped by timestamp", n, err)
	}
	pending, err := e.store.ClaimOutbox(ctx, f.BookID, 10, time.Minute)
	if err != nil || len(pending) != 0 {
		t.Fatal("acknowledged event still pending", err)
	}
}
func TestOutboxPublishersClaimOneOwner(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	results := make(chan []cockroach.LeasedFact, 6)
	failures := make(chan error, 6)
	start := make(chan struct{})
	for range 6 {
		wg.Go(func() {
			<-start
			facts, err := e.store.ClaimOutbox(ctx, e.fixture.BookID, 1, time.Minute)
			results <- facts
			failures <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	close(failures)
	count := 0
	for facts := range results {
		count += len(facts)
	}
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("expected one delivery owner, got %d", count)
	}
}
