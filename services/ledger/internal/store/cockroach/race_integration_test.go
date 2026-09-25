package cockroach_test

import (
	"context"
	"crypto/ed25519"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

// orderedRaceStore controls only scheduling. Every read, lock, write, retry and
// commit is performed by the real CockroachDB adapter. The first command keeps
// its authoritative row locks until the second transaction reaches Load, so
// both legal orderings are exercised without hoping a scheduler chooses them.
// These hooks live only in tests and are not linked into the serving binary.
type orderedRaceStore struct {
	application.Store
	first                     domain.ID
	locked, contender         chan struct{}
	lockedOnce, contenderOnce sync.Once
}
type orderedRaceTransaction struct {
	application.Transaction
	race *orderedRaceStore
}

func (s *orderedRaceStore) Transact(ctx context.Context, fn func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	return s.Store.Transact(ctx, func(tx application.Transaction) (application.Receipt, error) {
		return fn(&orderedRaceTransaction{Transaction: tx, race: s})
	})
}
func (tx *orderedRaceTransaction) Load(ctx context.Context, cmd application.Command, now time.Time) (application.State, error) {
	if cmd.OperationID != tx.race.first {
		tx.race.contenderOnce.Do(func() { close(tx.race.contender) })
		return tx.Transaction.Load(ctx, cmd, now)
	}
	state, err := tx.Transaction.Load(ctx, cmd, now)
	if err != nil {
		return state, err
	}
	tx.race.lockedOnce.Do(func() { close(tx.race.locked) })
	select {
	case <-tx.race.contender:
		return state, nil
	case <-ctx.Done():
		return application.State{}, ctx.Err()
	}
}

// Results retain input order. first selects which command holds the conflicting
// locks before the other transaction attempts to load authoritative state.
func orderedCommands(t *testing.T, e *environment, first int, commands ...application.Command) []application.Receipt {
	t.Helper()
	if len(commands) != 2 || first < 0 || first > 1 {
		t.Fatal("ordered race needs two commands and a valid first index")
	}
	store := &orderedRaceStore{Store: e.store, first: commands[first].OperationID, locked: make(chan struct{}), contender: make(chan struct{})}
	service := application.New(store, application.Trust{
		GrantKeys:    map[string]ed25519.PublicKey{"test-grant": e.grantKey.Public().(ed25519.PublicKey)},
		EvidenceKeys: map[string]ed25519.PublicKey{"test-evidence": e.evidenceKey.Public().(ed25519.PublicKey)},
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	answers := []chan referenceAnswer{make(chan referenceAnswer, 1), make(chan referenceAnswer, 1)}
	run := func(i int) {
		r, err := service.Execute(ctx, e.caller, commands[i])
		answers[i] <- referenceAnswer{Receipt: r, Error: err}
	}
	go run(first)
	select {
	case <-store.locked:
	case early := <-answers[first]:
		t.Fatalf("first command returned before acquiring locks: %s %v", early.Receipt.Outcome, early.Error)
	case <-ctx.Done():
		t.Fatal("first command did not acquire authoritative locks", ctx.Err())
	}
	go run(1 - first)
	result := make([]application.Receipt, 2)
	for i := range answers {
		select {
		case answer := <-answers[i]:
			if answer.Error != nil {
				t.Fatalf("race command %d: %v", i, answer.Error)
			}
			result[i] = answer.Receipt
		case <-ctx.Done():
			t.Fatal("ordered race did not finish", ctx.Err())
		}
	}
	return result
}

type expectedRaceState struct {
	// Exact stored debits, credits and held, in that order. Checking the raw
	// aggregates avoids using the production posting arithmetic as the oracle.
	Accounts           map[domain.ID][3]string
	Reserved, Consumed string
	Counts             map[string]int
}

func assertRaceState(t *testing.T, e *environment, want expectedRaceState) application.Snapshot {
	t.Helper()
	snapshot, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tables["account_balances"]) != len(want.Accounts) {
		t.Fatal("unexpected balance row count")
	}
	for _, row := range snapshot.Tables["account_balances"] {
		id := domain.ID(row["account_id"].(string))
		expected, exists := want.Accounts[id]
		if !exists || row["debits"] != expected[0] || row["credits"] != expected[1] || row["held"] != expected[2] {
			t.Fatalf("account %s: got debits=%v credits=%v held=%v; want %v", id, row["debits"], row["credits"], row["held"], expected)
		}
	}
	usage := snapshot.Tables["limit_usage"]
	if len(usage) != 1 || usage[0]["owner_id"] != string(e.fixture.OwnerA) || usage[0]["reserved"] != want.Reserved || usage[0]["consumed"] != want.Consumed {
		t.Fatalf("unexpected hard-limit allocation: %+v", usage)
	}
	for table, count := range want.Counts {
		if snapshot.Counts[table] != count {
			t.Fatalf("%s: got %d rows, want %d", table, snapshot.Counts[table], count)
		}
	}
	return snapshot
}

func assertRaceReplay(t *testing.T, e *environment, before application.Snapshot, commands []application.Command, original []application.Receipt) {
	t.Helper()
	for i, command := range commands {
		for range 2 {
			replay := e.execute(t, command)
			if !reflect.DeepEqual(replay, original[i]) {
				t.Fatalf("command %d changed its durable receipt on replay", i)
			}
		}
		lookup, err := e.service.GetOperation(context.Background(), e.caller, command.BookID, command.OperationID)
		if err != nil || !reflect.DeepEqual(lookup, original[i]) {
			t.Fatalf("command %d receipt lookup differs: %v", i, err)
		}
	}
	after, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot cut advances; every exported financial row must stay fixed.
	if !reflect.DeepEqual(before.Tables, after.Tables) || !reflect.DeepEqual(before.Counts, after.Counts) || !reflect.DeepEqual(before.Digests, after.Digests) {
		t.Fatal("replay changed financial history or aggregates")
	}
}

func TestDeterministicHoldVersusSpend(t *testing.T) {
	for first, name := range []string{"hold_wins", "transfer_wins"} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			f := e.fixture
			reserve := e.reserve(t)
			reserve.Reserve.Terms = e.terms(t, "60000", "1000", f.Policy1000)
			transfer := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal,
				Transfer: &application.Transfer{Terms: e.terms(t, "60000", "1000", f.Policy1000)}}
			commands := []application.Command{reserve, transfer}
			results := orderedCommands(t, e, first, commands...)
			if results[first].Outcome != "APPLIED" || results[1-first].Outcome != "REJECTED" || results[1-first].Reason != "INSUFFICIENT_FUNDS" {
				t.Fatalf("capacity was not awarded exactly once: %+v", results)
			}
			want := expectedRaceState{
				Accounts: map[domain.ID][3]string{f.WalletA: {"0", "100000", "61000"}, f.WalletB: {"0", "0", "0"}, f.FeeAccount: {"0", "0", "0"}, f.Pool: {"100000", "0", "60000"}},
				Reserved: "60000", Consumed: "0",
				Counts: map[string]int{"journals": 1, "journal_lines": 2, "financial_operations": 3, "outbox_facts": 3, "business_claims": 2, "holds": 1, "hold_events": 1, "limit_events": 1, "account_events": 6},
			}
			if first == 1 {
				want.Accounts[f.WalletA], want.Accounts[f.WalletB], want.Accounts[f.FeeAccount], want.Accounts[f.Pool] = [3]string{"61000", "100000", "0"}, [3]string{"0", "60000", "0"}, [3]string{"0", "1000", "0"}, [3]string{"100000", "0", "0"}
				want.Reserved, want.Consumed = "0", "60000"
				want.Counts["journals"], want.Counts["journal_lines"], want.Counts["holds"], want.Counts["hold_events"], want.Counts["account_events"] = 2, 5, 0, 0, 7
			}
			snapshot := assertRaceState(t, e, want)
			for _, claim := range snapshot.Tables["business_claims"] {
				winner := claim["payment_id"] == string(commands[first].PaymentID())
				state := "REJECTED"
				if winner {
					state = "APPLIED"
					if first == 0 {
						state = "RESERVED"
					}
				}
				if claim["state"] != state {
					t.Fatal("execution claim does not reflect the winning operation", claim)
				}
			}
			assertRaceReplay(t, e, snapshot, commands, results)
			verifyReferenceSnapshot(t, "race-"+name, snapshot)
		})
	}
}
