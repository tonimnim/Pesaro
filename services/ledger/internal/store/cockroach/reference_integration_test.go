package cockroach_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

type referenceCase struct {
	Input   referenceInstruction
	Command application.Command
}
type referenceAnswer struct {
	Receipt application.Receipt
	Error   error
}

func fixedClock(e *environment, now *time.Time) {
	e.service = application.New(e.store, application.Trust{
		GrantKeys:    map[string]ed25519.PublicKey{"test-grant": e.grantKey.Public().(ed25519.PublicKey)},
		EvidenceKeys: map[string]ed25519.PublicKey{"test-evidence": e.evidenceKey.Public().(ed25519.PublicKey)},
	}, func() time.Time { return *now })
}

func (e *environment) referenceCase(t *testing.T, input referenceInstruction, now time.Time, reserves map[string]application.Command) referenceCase {
	t.Helper()
	f := e.fixture
	cmd := application.Command{BookID: f.BookID, OperationID: domain.ID(input.Key)}
	if input.Kind == "transfer" || input.Kind == "reserve" {
		policy := map[int64]domain.ID{0: f.PolicyZero, 100: f.Policy100, 200: f.Policy200, 1000: f.Policy1000}[input.Fee]
		terms := e.terms(t, strconv.FormatInt(input.Principal, 10), strconv.FormatInt(input.Fee, 10), policy)
		terms.PaymentID = domain.ID(input.Payment)
		if input.Source == 1 {
			terms.SourceID, terms.BeneficiaryID = f.WalletB, f.WalletA
			terms.Grant.Claims.SubjectID = f.OwnerB
		}
		g := terms.Grant.Claims
		g.PaymentID, g.SourceID, g.BeneficiaryID = terms.PaymentID, terms.SourceID, terms.BeneficiaryID
		g.NotBefore, g.ExpiresAt = now.Add(-time.Minute), now.Add(time.Hour)
		var err error
		terms.Grant, err = application.SignGrant(g, e.grantKey)
		if err != nil {
			t.Fatal(err)
		}
		if input.Kind == "transfer" {
			cmd.Kind, cmd.Transfer = application.TransferInternal, &application.Transfer{Terms: terms}
		} else {
			cmd.Kind, cmd.Reserve = application.ReservePayout, &application.Reserve{Terms: terms, HoldID: domain.ID(input.Hold), AttemptID: domain.NewID(), PoolID: f.Pool, ExpiresAt: now.Add(5 * time.Minute)}
			reserves[input.Hold] = cmd
		}
		return referenceCase{input, cmd}
	}
	reserve := reserves[input.Hold]
	ref := application.HoldRef{PaymentID: reserve.Reserve.Terms.PaymentID, HoldID: domain.ID(input.Hold), AttemptID: reserve.Reserve.AttemptID, ExpectedVersion: input.Version}
	switch input.Kind {
	case "expose":
		cmd.Kind, cmd.Expose = application.MarkHoldExposed, &application.Expose{Ref: ref, GrantID: reserve.Reserve.Terms.Grant.Claims.ID}
	case "capture":
		cmd.Kind, cmd.Capture = application.CapturePayout, &application.Capture{Ref: ref, Evidence: e.evidenceAt(t, reserve, true, false, now)}
	case "release":
		cmd.Kind, cmd.Release = application.ReleasePayout, &application.Release{Ref: ref, Reason: "CANCEL"}
		if input.Fenced {
			evidence := e.evidenceAt(t, reserve, false, true, now)
			cmd.Release.Reason, cmd.Release.Evidence = "FINAL_FAILURE", &evidence
		}
	}
	return referenceCase{input, cmd}
}

func (e *environment) evidenceAt(t *testing.T, reserve application.Command, success, fenced bool, now time.Time) application.SignedEvidence {
	t.Helper()
	evidence := e.evidence(t, reserve, success, fenced)
	evidence.Claims.ObservedAt = now
	signed, err := application.SignEvidence(evidence.Claims, e.evidenceKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// Every batch starts behind one barrier. Enumerating all six serial orders of
// three calls lets the independent oracle accept any legal database ordering,
// while rejecting a result/state combination no serial execution can produce.
func TestRandomizedConcurrentReferenceModel(t *testing.T) {
	for _, seed := range []uint64{17, 73, 260925} {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			e := setup(t)
			now := time.Now().UTC().Truncate(time.Second)
			fixedClock(e, &now)
			cap := int64(200000)
			if seed == 73 {
				cap = 30000
			}
			for _, owner := range []domain.ID{e.fixture.OwnerA, e.fixture.OwnerB} {
				limit, _ := domain.ParseWide(strconv.FormatInt(cap, 10))
				cmd := application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.SetAccountControl,
					Control: &application.SetControl{Key: application.ControlKey{Kind: "SUBJECT", ID: owner}, ExpectedVersion: 1, DailyCap: limit, Reason: "REFERENCE_FIXTURE"}}
				if e.execute(t, cmd).Outcome != "APPLIED" {
					t.Fatal("cap fixture")
				}
			}
			model := newReferenceBook(cap)
			rng := rand.New(rand.NewPCG(seed, seed^0xabc987))
			reserves := map[string]application.Command{}
			history := []referenceCase{}
			receipts := map[string]application.Receipt{}
			// An independently chosen fixed seed provides both wallets with funds
			// and one exposed obligation before generated interleavings begin.
			initialHold, initialPayment := string(domain.NewID()), string(domain.NewID())
			for _, input := range []referenceInstruction{
				{Kind: "transfer", Source: 0, Principal: 10000, Fee: 0, Payment: string(domain.NewID())},
				{Kind: "reserve", Source: 0, Principal: 12000, Fee: 200, Hold: initialHold, Payment: initialPayment},
				{Kind: "expose", Hold: initialHold, Payment: initialPayment, Version: 1},
			} {
				input.Key, input.Bucket = string(domain.NewID()), now.Add(3*time.Hour).Format("2006-01-02")
				c := e.referenceCase(t, input, now, reserves)
				receipt := e.execute(t, c.Command)
				if model.apply(input) != "" || receipt.Outcome != "APPLIED" {
					t.Fatal("reference seed failed")
				}
				history, receipts[input.Key] = append(history, c), receipt
			}
			for batch := range 24 {
				cases := make([]referenceCase, 3)
				keys := make([]string, 0, len(model.Holds))
				seen := map[string]bool{}
				for _, old := range history {
					key := old.Input.Hold
					if _, exists := model.Holds[key]; exists && !seen[key] {
						keys = append(keys, key)
						seen[key] = true
					}
				}
				for j := range cases {
					input := referenceInstruction{Key: string(domain.NewID()), Payment: string(domain.NewID()), Hold: string(domain.NewID()),
						Source: rng.IntN(2), Principal: []int64{1, 500, 2000, 9000, 60000}[rng.IntN(5)], Fee: []int64{0, 100, 200, 1000}[rng.IntN(4)],
						Bucket: now.Add(3 * time.Hour).Format("2006-01-02")}
					choice := rng.IntN(10)
					if choice == 9 {
						cases[j] = history[rng.IntN(len(history))]
						if rng.IntN(2) == 0 {
							cases[j].Input.Key, cases[j].Command.OperationID = input.Key, domain.ID(input.Key)
						}
						continue
					}
					switch {
					case batch == 0 || choice >= 5 && len(keys) != 0:
						key := keys[rng.IntN(len(keys))]
						if batch == 0 {
							key = initialHold
						}
						hold := model.Holds[key]
						input.Hold, input.Payment, input.Version = key, hold.Instruction.Payment, hold.Version
						input.Kind = []string{"expose", "capture", "release"}[rng.IntN(3)]
						if batch == 0 {
							input.Kind = []string{"capture", "capture", "release"}[j]
						}
						input.Fenced = hold.State == "EXPOSED"
					case choice >= 3:
						input.Kind = "reserve"
					default:
						input.Kind = "transfer"
					}
					cases[j] = e.referenceCase(t, input, now, reserves)
				}
				start := make(chan struct{})
				answers := make([]referenceAnswer, len(cases))
				done := make(chan int, len(cases))
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				for j, c := range cases {
					go func() {
						<-start
						answers[j].Receipt, answers[j].Error = e.service.Execute(ctx, e.caller, c.Command)
						done <- j
					}()
				}
				close(start)
				for range cases {
					<-done
				}
				cancel()
				for j, answer := range answers {
					if answer.Error != nil {
						t.Fatalf("seed=%d batch=%d call=%d: %v", seed, batch, j, answer.Error)
					}
					key := cases[j].Input.Key
					if old, ok := receipts[key]; ok && !reflect.DeepEqual(old, answer.Receipt) {
						t.Fatalf("seed=%d batch=%d receipt changed on retry", seed, batch)
					}
					receipts[key] = answer.Receipt
				}
				snapshot, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
				if err != nil {
					t.Fatal(err)
				}
				var matched *referenceBook
				for _, order := range [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
					candidate, valid := model.clone(), true
					for _, j := range order {
						reason := candidate.apply(cases[j].Input)
						if (reason == "") != (answers[j].Receipt.Outcome == "APPLIED") {
							valid = false
						}
					}
					if valid && referenceMatches(e, candidate, snapshot) == nil {
						matched = candidate
						break
					}
				}
				if matched == nil {
					instructions := []referenceInstruction{cases[0].Input, cases[1].Input, cases[2].Input}
					inputs, _ := json.Marshal(instructions)
					t.Fatalf("seed=%d batch=%d has no legal serial explanation; cases=%s answers=%+v", seed, batch, inputs, answers)
				}
				model = matched
				history = append(history, cases...)
			}
			snapshot, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
			if err != nil {
				t.Fatal(err)
			}
			verifyReferenceSnapshot(t, fmt.Sprintf("reference-seed-%d", seed), snapshot)
			t.Logf("seed=%d: 24 barrier batches x 3 calls; all receipts and final positions matched; holds=%d operations=%d", seed, len(model.Holds), len(model.Receipts))
		})
	}
}

func referenceMatches(e *environment, model *referenceBook, snapshot application.Snapshot) error {
	f, p := e.fixture, model.position()
	index := map[string]int{string(f.WalletA): 0, string(f.WalletB): 1, string(f.FeeAccount): 2, string(f.Pool): 3}
	if len(snapshot.Tables["account_balances"]) != 4 || snapshot.Counts["journals"] != p.Journals || snapshot.Counts["holds"] != len(model.Holds) || snapshot.Counts["business_claims"] != len(model.Claims) {
		return fmt.Errorf("financial row counts differ")
	}
	// Fixture plus two cap controls exist before the reference command stream.
	for _, table := range []string{"financial_operations", "outbox_facts"} {
		if snapshot.Counts[table] != len(model.Receipts)+3 {
			return fmt.Errorf("%s count differs", table)
		}
	}
	for _, row := range snapshot.Tables["account_balances"] {
		i, ok := index[row["account_id"].(string)]
		if !ok {
			return fmt.Errorf("unknown account")
		}
		debits, d := new(big.Int).SetString(row["debits"].(string), 10)
		credits, c := new(big.Int).SetString(row["credits"].(string), 10)
		if !d || !c {
			return fmt.Errorf("invalid exact aggregate")
		}
		posted := new(big.Int).Sub(credits, debits)
		if i == 3 {
			posted.Neg(posted)
		}
		if posted.Cmp(p.Posted[i]) != 0 || row["held"] != p.Held[i].String() {
			return fmt.Errorf("account %d position differs", i)
		}
	}
	for _, row := range snapshot.Tables["holds"] {
		h := model.Holds[row["hold_id"].(string)]
		if row["state"] != h.State || row["version"] != strconv.FormatInt(h.Version, 10) {
			return fmt.Errorf("hold state/version differs")
		}
	}
	for _, row := range snapshot.Tables["limit_usage"] {
		i := 0
		if row["owner_id"] == string(f.OwnerB) {
			i = 1
		} else if row["owner_id"] != string(f.OwnerA) {
			return fmt.Errorf("unexpected usage owner")
		}
		bucket := row["bucket"].(string)
		if row["reserved"] != bucketValue(p.Reserved[i], bucket).String() || row["consumed"] != bucketValue(p.Consumed[i], bucket).String() {
			return fmt.Errorf("usage differs")
		}
		delete(p.Reserved[i], bucket)
		delete(p.Consumed[i], bucket)
	}
	for i := range p.Reserved {
		for _, values := range []map[string]*big.Int{p.Reserved[i], p.Consumed[i]} {
			for _, value := range values {
				if value.Sign() != 0 {
					return fmt.Errorf("missing usage")
				}
			}
		}
	}
	return nil
}

func verifyReferenceSnapshot(t *testing.T, name string, snapshot application.Snapshot) {
	t.Helper()
	dir := t.TempDir()
	if evidence := os.Getenv("PESAR_LEDGER_EVIDENCE_DIR"); evidence != "" {
		dir = evidence
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name+".json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root missing")
		}
		root = parent
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "./services/reconciliation/cmd/ledger-verify", "-snapshot", path)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent verifier: %v: %s", err, output)
	}
	if err = os.WriteFile(filepath.Join(dir, name+"-report.json"), output, 0600); err != nil {
		t.Fatal(err)
	}
}
