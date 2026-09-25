package cockroach

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

// Test-only wrapper, absent from the serving binary. The database raises the
// actual serialization error after every financial row has been written.
// force_retry is also used by upstream's transaction logic tests:
// https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/logictest/testdata/logic_test/crdb_internal
type retryProbeStore struct {
	*Store
	failures       int
	seen           []application.Receipt
	injectionError error
}

func (s *retryProbeStore) Transact(ctx context.Context, fn func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	return s.Store.Transact(ctx, func(transaction application.Transaction) (application.Receipt, error) {
		result, err := fn(transaction)
		if err != nil {
			return result, err
		}
		s.seen = append(s.seen, result)
		if len(s.seen) <= s.failures {
			if _, err = transaction.(*tx).Exec(ctx, "SET LOCAL allow_unsafe_internals = on"); err != nil {
				return result, err
			}
			_, err = transaction.(*tx).Exec(ctx, "SELECT crdb_internal.force_retry('1h'::INTERVAL)")
			if sqlCode(err) != "40001" {
				s.injectionError = err
				return result, fmt.Errorf("database did not produce the requested serialization abort: %w", err)
			}
		}
		return result, err
	})
}

func TestDatabaseForcedSerializationRollback(t *testing.T) {
	adminDSN := os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL")
	if adminDSN == "" {
		t.Skip("requires prepared real CockroachDB")
	}
	for _, failures := range []int{2, 3} {
		name := "recover_after_two_aborts"
		if failures == 3 {
			name = "bounded_retry_exhaustion"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			admin, err := Open(ctx, adminDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			fixture, err := admin.BootstrapSynthetic(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Internal failure injection is opted into only in test transactions.
			// Exercise the identical adapter under test-only admin credentials;
			// do not enable unsafe internals in the runtime. Other integration tests
			// execute their financial commands with restricted runtime credentials.
			store, err := Open(ctx, adminDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			store.retries = 3
			probe := &retryProbeStore{Store: store, failures: failures}
			public, private, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			principal, _ := domain.ParseAmount("10000")
			fee, _ := domain.ParseAmount("100")
			payment, quote := domain.NewID(), domain.NewID()
			grant, err := application.SignGrant(application.Grant{ID: domain.NewID(), Issuer: "retry-test", BookID: fixture.BookID,
				PaymentID: payment, SubjectID: fixture.OwnerA, SourceID: fixture.WalletA, BeneficiaryID: fixture.WalletB,
				Principal: principal, Fee: fee, PolicyID: fixture.Policy100, QuoteID: quote, Currency: "KES", SubjectEpoch: 1, AccountEpoch: 1,
				NotBefore: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour)}, private)
			if err != nil {
				t.Fatal(err)
			}
			command := application.Command{BookID: fixture.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal,
				Transfer: &application.Transfer{Terms: application.Terms{PaymentID: payment, SourceID: fixture.WalletA, BeneficiaryID: fixture.WalletB,
					Principal: principal, Fee: fee, Currency: "KES", PolicyID: fixture.Policy100, QuoteID: quote, Grant: grant}}}
			service := application.New(probe, application.Trust{GrantKeys: map[string]ed25519.PublicKey{"retry-test": public}}, nil)
			caller := application.Caller{Identity: "retry-test", Books: map[domain.ID]bool{fixture.BookID: true}, Permissions: map[string]bool{"spend": true}}
			receipt, err := service.Execute(ctx, caller, command)
			if probe.injectionError != nil {
				t.Fatal("fault injection unavailable", probe.injectionError)
			}
			wantBalance, wantCount := "89900", 2
			if failures == 3 {
				if !errors.Is(err, application.ErrUnavailable) {
					t.Fatal("retry limit not enforced", err)
				}
				wantBalance, wantCount = "100000", 1
				op, lookupErr := store.Operation(ctx, fixture.BookID, command.OperationID)
				if lookupErr != nil || op != nil {
					t.Fatal("aborted operation persisted", lookupErr)
				}
			} else if err != nil || receipt.Outcome != "APPLIED" {
				t.Fatal(receipt, err)
			}
			if len(probe.seen) != 3 {
				t.Fatal("wrong retry count", len(probe.seen))
			}
			for _, attempt := range probe.seen {
				if attempt.EventID != probe.seen[0].EventID || attempt.JournalID != probe.seen[0].JournalID {
					t.Fatal("retry changed financial identities")
				}
			}
			_, balance, err := store.Balance(ctx, fixture.BookID, fixture.WalletA)
			if err != nil {
				t.Fatal(err)
			}
			posted, err := balance.Totals.Posted(domain.Wallet)
			if err != nil || posted.String() != wantBalance {
				t.Fatal("aborted money escaped rollback", posted, err)
			}
			snapshot, err := store.Snapshot(ctx, fixture.BookID)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"journals", "financial_operations", "outbox_facts"} {
				if snapshot.Counts[table] != wantCount {
					t.Fatal("aborted history escaped rollback", table, snapshot.Counts)
				}
			}
			if snapshot.Counts["business_claims"] != wantCount-1 || snapshot.Counts["limit_usage"] != wantCount-1 {
				t.Fatal("aborted admission escaped rollback", snapshot.Counts)
			}
		})
	}
}
