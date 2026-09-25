package cockroach_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
)

type crashChild struct {
	input    *json.Encoder
	points   chan cockroach.CrashCheckpoint
	readErr  chan error
	finished chan struct{}
	waitErr  error
	stderr   bytes.Buffer
	command  *exec.Cmd
	stdin    io.WriteCloser
	stdout   io.ReadCloser
	once     sync.Once
}

func startWriteCrashChild(t *testing.T, instruction cockroach.CrashInstruction) *crashChild {
	t.Helper()
	child := &crashChild{points: make(chan cockroach.CrashCheckpoint), readErr: make(chan error, 1), finished: make(chan struct{})}
	child.command = exec.Command(os.Args[0], "-test.run=^TestLedgerWriteCrashChild$")
	// Never pass fixture/migration authority or private signing keys to the
	// child. Public trust and the exact command travel over stdin, not argv.
	for _, entry := range os.Environ() {
		name := strings.ToUpper(strings.SplitN(entry, "=", 2)[0])
		if !strings.HasPrefix(name, "PESARO_LEDGER_") && !strings.HasPrefix(name, "PESAR_LEDGER_") {
			child.command.Env = append(child.command.Env, entry)
		}
	}
	child.command.Env = append(child.command.Env, "PESAR_LEDGER_WRITE_CRASH_CHILD=1", "PESAR_LEDGER_DATABASE_URL="+os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL"))
	var err error
	child.stdin, err = child.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.stdout, err = child.command.StdoutPipe()
	if err != nil {
		_ = child.stdin.Close()
		t.Fatal(err)
	}
	child.command.Stderr = &child.stderr
	if err = child.command.Start(); err != nil {
		_ = child.stdin.Close()
		_ = child.stdout.Close()
		t.Fatal(err)
	}
	go func() {
		child.waitErr = child.command.Wait()
		close(child.finished)
	}()
	go func() {
		decoder := json.NewDecoder(child.stdout)
		for {
			var point cockroach.CrashCheckpoint
			if err := decoder.Decode(&point); err != nil {
				child.readErr <- err
				return
			}
			select {
			case child.points <- point:
			case <-child.finished:
				return
			}
		}
	}()
	t.Cleanup(func() { child.kill(t) })
	child.input = json.NewEncoder(child.stdin)
	if err = child.input.Encode(instruction); err != nil {
		t.Fatal(err)
	}
	return child
}

func (c *crashChild) kill(t *testing.T) {
	t.Helper()
	c.once.Do(func() {
		if err := c.command.Process.Kill(); err != nil {
			t.Errorf("could not kill Ledger child: %v", err)
		}
		select {
		case <-c.finished:
			var exit *exec.ExitError
			if !errors.As(c.waitErr, &exit) || exit.Success() {
				t.Errorf("Ledger child did not terminate abnormally: %v; %s", c.waitErr, c.stderr.String())
			}
		case <-time.After(5 * time.Second):
			t.Error("killed Ledger child did not exit")
		}
		_ = c.stdin.Close()
		_ = c.stdout.Close()
	})
}

func (c *crashChild) next(t *testing.T) cockroach.CrashCheckpoint {
	t.Helper()
	select {
	case point := <-c.points:
		return point
	case err := <-c.readErr:
		t.Fatalf("Ledger child checkpoint stream: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("Ledger child did not reach checkpoint")
	}
	return cockroach.CrashCheckpoint{}
}

type crashState struct {
	Financial application.Snapshot
	Delivery  []string
}

func readCrashState(t *testing.T, store *cockroach.Store, book domain.ID) crashState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	snapshot, err := store.Snapshot(ctx, book)
	if err != nil {
		t.Fatal(err)
	}
	// Each consistent cut has a new timestamp even if no financial row changes.
	snapshot.Cut = ""
	delivery, err := cockroach.CrashDeliverySnapshot(ctx, store, book)
	if err != nil {
		t.Fatal(err)
	}
	return crashState{snapshot, delivery}
}

type crashEvidence struct {
	Boundary  string `json:"boundary"`
	SQL       string `json:"sql,omitempty"`
	Caller    string `json:"caller_outcome"`
	Persisted string `json:"persisted_outcome"`
	Recovery  string `json:"recovery"`
	Duration  string `json:"recovery_duration"`
}

// Each restart reissues exactly the original command. We advance one write
// farther per process until every SQL mutation and both commit edges have been
// killed. Pre-commit recovery must reproduce the entire original state; after
// commit, lookup and repeated execution must preserve the durable receipt and
// every financial/delivery row. No timer determines the injection location.
func exerciseWriteCrashes(t *testing.T, e *environment, command application.Command, outcome, reason string) application.Snapshot {
	t.Helper()
	instruction := cockroach.CrashInstruction{Command: command, Caller: e.caller, Now: time.Now().UTC(), Trust: application.Trust{
		GrantKeys:    map[string]ed25519.PublicKey{"test-grant": e.grantKey.Public().(ed25519.PublicKey)},
		EvidenceKeys: map[string]ed25519.PublicKey{"test-evidence": e.evidenceKey.Public().(ed25519.PublicKey)},
	}}
	baseline := readCrashState(t, e.store, command.BookID)
	var trace []cockroach.CrashCheckpoint
	var evidence []crashEvidence
	var committed *application.Receipt
	for target := 0; target < 100; target++ {
		child := startWriteCrashChild(t, instruction)
		var point cockroach.CrashCheckpoint
		for index := 0; index <= target; index++ {
			point = child.next(t)
			if point.Phase == "response" {
				t.Fatal("child delivered a response before the requested crash")
			}
			if index < target {
				if point.Phase != trace[index].Phase || point.SQL != trace[index].SQL {
					t.Fatal("recovery changed the write sequence", index, point, trace[index])
				}
				if err := child.input.Encode(true); err != nil {
					t.Fatal(err)
				}
			}
		}
		trace = append(trace, point)
		child.kill(t)
		started := time.Now()
		state := readCrashState(t, e.store, command.BookID)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		op, err := e.store.Operation(ctx, command.BookID, command.OperationID)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		record := crashEvidence{Boundary: point.Phase, SQL: point.SQL, Caller: "UNKNOWN (process killed before response)", Recovery: "restart; lookup and reissue original operation", Duration: time.Since(started).String()}
		if point.Phase == "after_commit" {
			if op == nil || point.Receipt == nil || !reflect.DeepEqual(op.Receipt, *point.Receipt) {
				t.Fatal("committed receipt lost or changed after process kill", op)
			}
			committed = &op.Receipt
			record.Persisted = op.Receipt.Outcome
		} else {
			if op != nil || !reflect.DeepEqual(state, baseline) {
				t.Fatalf("partial financial/delivery state escaped rollback at %s (%s)", point.Phase, point.SQL)
			}
			record.Persisted = "ABSENT; all financial and delivery rows unchanged"
		}
		evidence = append(evidence, record)
		t.Logf("kill %02d: %s %s; persisted=%s; recovery=%s", target, point.Phase, point.SQL, record.Persisted, record.Duration)
		if committed != nil {
			break
		}
	}
	if committed == nil || committed.Outcome != outcome || committed.Reason != reason {
		t.Fatal("original operation did not recover its expected terminal receipt", committed)
	}
	if trace[0].Phase != "before_save" || trace[len(trace)-2].Phase != "before_commit" || trace[len(trace)-3].SQL != "INSERT INTO outbox_delivery(book_id,event_id) VALUES($1,$2)" {
		t.Fatal("missing save/receipt/outbox/commit boundary coverage", trace)
	}
	after := readCrashState(t, e.store, command.BookID)
	for range 2 {
		if replay := e.execute(t, command); !reflect.DeepEqual(replay, *committed) {
			t.Fatal("original-identity replay changed the receipt", replay)
		}
	}
	if !reflect.DeepEqual(readCrashState(t, e.store, command.BookID), after) {
		t.Fatal("replay duplicated financial or delivery effects")
	}
	for _, table := range []string{"financial_operations", "outbox_facts"} {
		if after.Financial.Counts[table] != baseline.Financial.Counts[table]+1 {
			t.Fatal("expected exactly one durable operation and outbox fact", table)
		}
	}
	if len(after.Delivery) != len(baseline.Delivery)+1 {
		t.Fatal("expected exactly one durable delivery row")
	}
	name := "write-crash-" + strings.ReplaceAll(t.Name(), "/", "-")
	// Keep the actual export cut in verifier artifacts, not the comparison copy.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	snapshot, err := e.store.Snapshot(ctx, command.BookID)
	if err != nil {
		t.Fatal(err)
	}
	verifyReferenceSnapshot(t, name, snapshot)
	if dir := os.Getenv("PESAR_LEDGER_EVIDENCE_DIR"); dir != "" {
		data, err := json.MarshalIndent(struct {
			OperationID domain.ID       `json:"operation_id"`
			Points      []crashEvidence `json:"points"`
		}{command.OperationID, evidence}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name+"-crashes.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func TestProcessCrashEveryTransferWrite(t *testing.T) {
	e := setup(t)
	f := e.fixture
	command := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	snapshot := exerciseWriteCrashes(t, e, command, "APPLIED", "")
	for id, expected := range map[domain.ID]string{f.WalletA: "89900", f.WalletB: "10000", f.FeeAccount: "100"} {
		purpose := domain.Wallet
		if id == f.FeeAccount {
			purpose = domain.FeeIncome
		}
		posted, err := e.balance(t, id).Posted(purpose)
		if err != nil || posted.String() != expected {
			t.Fatal("principal or fee was lost/duplicated", id, posted, err)
		}
	}
	if snapshot.Counts["journals"] != 2 || snapshot.Counts["business_claims"] != 1 {
		t.Fatal("wrong transfer history", snapshot.Counts)
	}
}
