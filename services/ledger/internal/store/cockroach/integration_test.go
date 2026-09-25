package cockroach_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
)

type environment struct {
	admin       *cockroach.Store
	store       *cockroach.Store
	fixture     cockroach.Fixture
	service     *application.Service
	caller      application.Caller
	grantKey    ed25519.PrivateKey
	evidenceKey ed25519.PrivateKey
}

func TestConcurrentSpendingAndPermanentIdentity(t *testing.T) {
	e := setup(t)
	f := e.fixture
	commands := []application.Command{
		{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "60000", "1000", f.Policy1000)}},
		{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "60000", "1000", f.Policy1000)}},
	}
	start := make(chan struct{})
	type answer struct {
		r   application.Receipt
		err error
	}
	answers := make(chan answer, 2)
	for _, cmd := range commands {
		go func() {
			<-start
			r, err := e.service.Execute(context.Background(), e.caller, cmd)
			answers <- answer{r, err}
		}()
	}
	close(start)
	applied, rejected := 0, 0
	for range commands {
		a := <-answers
		if a.err != nil {
			t.Fatal(a.err)
		}
		switch a.r.Outcome {
		case "APPLIED":
			applied++
		case "REJECTED":
			rejected++
			if a.r.Reason != "INSUFFICIENT_FUNDS" {
				t.Fatal(a.r.Reason)
			}
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatal(applied, rejected)
	}
	a, _ := e.balance(t, f.WalletA).Available(domain.Wallet)
	if a.String() != "39000" {
		t.Fatal(a)
	}
	for _, cmd := range commands {
		original := e.execute(t, cmd)
		replay := e.execute(t, cmd)
		if replay.EventID != original.EventID {
			t.Fatal("replay")
		}
		cmd.OperationID = domain.NewID()
		r := e.execute(t, cmd)
		if r.Outcome != "REJECTED" || r.Reason != "BUSINESS_ALREADY_DECIDED" {
			t.Fatal(r)
		}
	}
}
func TestConcurrentSameOperationAndMismatch(t *testing.T) {
	e := setup(t)
	f := e.fixture
	cmd := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	start := make(chan struct{})
	type answer struct {
		r   application.Receipt
		err error
	}
	answers := make(chan answer, 12)
	for range 12 {
		go func() {
			<-start
			r, err := e.service.Execute(context.Background(), e.caller, cmd)
			answers <- answer{r, err}
		}()
	}
	close(start)
	var event domain.ID
	for range 12 {
		a := <-answers
		if a.err != nil {
			t.Fatal(a.err)
		}
		if event == "" {
			event = a.r.EventID
		}
		if a.r.EventID != event || a.r.Outcome != "APPLIED" {
			t.Fatal(a.r)
		}
	}
	a, _ := e.balance(t, f.WalletA).Available(domain.Wallet)
	if a.String() != "89900" {
		t.Fatal(a)
	}
	changed := *cmd.Transfer
	changed.Terms.Principal = amt(t, "9999")
	cmd.Transfer = &changed
	if _, err := e.service.Execute(context.Background(), e.caller, cmd); !errors.Is(err, application.ErrConflict) {
		t.Fatal("material conflict", err)
	}
}
func (e *environment) reserve(t *testing.T) application.Command {
	f := e.fixture
	return application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.ReservePayout, Reserve: &application.Reserve{Terms: e.terms(t, "20000", "200", f.Policy200), HoldID: domain.NewID(), AttemptID: domain.NewID(), PoolID: f.Pool, ExpiresAt: time.Now().UTC().Add(5 * time.Minute)}}
}
func (e *environment) expose(reserve application.Command) application.Command {
	r := reserve.Reserve
	return application.Command{BookID: reserve.BookID, OperationID: domain.NewID(), Kind: application.MarkHoldExposed, Expose: &application.Expose{Ref: application.HoldRef{PaymentID: r.Terms.PaymentID, HoldID: r.HoldID, AttemptID: r.AttemptID, ExpectedVersion: 1}, GrantID: r.Terms.Grant.Claims.ID}}
}
func (e *environment) evidence(t *testing.T, reserve application.Command, success, fenced bool) application.SignedEvidence {
	t.Helper()
	r := reserve.Reserve
	state := "FINAL_FAILURE"
	if success {
		state = "FINAL_SUCCESS"
	}
	ev := application.Evidence{ID: domain.NewID(), Issuer: "test-evidence", BookID: reserve.BookID, PaymentID: r.Terms.PaymentID, HoldID: r.HoldID, AttemptID: r.AttemptID, ProviderAccountID: e.fixture.ProviderAccount, BeneficiaryID: r.Terms.BeneficiaryID, Principal: r.Terms.Principal, Currency: "KES", CapabilityID: e.fixture.Capability, FinalState: state, SubmissionFenced: fenced, ObservedAt: time.Now().UTC(), SourceDigest: strings.Repeat("a", 64)}
	signed, err := application.SignEvidence(ev, e.evidenceKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}
func TestPayoutCaptureAfterFreezeAndLostReplyReplay(t *testing.T) {
	e := setup(t)
	f := e.fixture
	reserve := e.reserve(t)
	if r := e.execute(t, reserve); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	b := e.balance(t, f.WalletA)
	if b.Credits.String() != "100000" || b.Held.String() != "20200" {
		t.Fatal(b)
	}
	exposure := e.expose(reserve)
	if r := e.execute(t, exposure); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	cap, _ := domain.ParseWide("200000")
	freeze := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.SetAccountControl, Control: &application.SetControl{Key: application.ControlKey{Kind: "SUBJECT", ID: f.OwnerA}, ExpectedVersion: 1, DailyCap: cap, DebitFrozen: true, RevokeGrants: true, Reason: "TEST"}}
	if r := e.execute(t, freeze); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	ref := exposure.Expose.Ref
	ref.ExpectedVersion = 2
	capture := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.CapturePayout, Capture: &application.Capture{Ref: ref, Evidence: e.evidence(t, reserve, true, false)}}
	// Discard the first response, then recover the same durable operation.
	_ = e.execute(t, capture)
	r := e.execute(t, capture)
	if r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	b = e.balance(t, f.WalletA)
	a, _ := b.Available(domain.Wallet)
	if a.String() != "79800" || !b.Held.IsZero() {
		t.Fatal(b)
	}
	p, _ := e.balance(t, f.Pool).Posted(domain.ProviderPool)
	if p.String() != "80000" {
		t.Fatal(p)
	}
	fee, _ := e.balance(t, f.FeeAccount).Posted(domain.FeeIncome)
	if fee.String() != "200" {
		t.Fatal(fee)
	}
	capture.OperationID = domain.NewID()
	if r = e.execute(t, capture); r.Reason != "STATE_CONFLICT" {
		t.Fatal(r)
	}
}
func TestExposedReleaseNeedsFencedEvidence(t *testing.T) {
	e := setup(t)
	f := e.fixture
	reserve := e.reserve(t)
	if r := e.execute(t, reserve); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	expose := e.expose(reserve)
	if r := e.execute(t, expose); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	ref := expose.Expose.Ref
	ref.ExpectedVersion = 2
	cancel := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout, Release: &application.Release{Ref: ref, Reason: "CANCEL"}}
	if r := e.execute(t, cancel); r.Reason != "EXPOSED_REQUIRES_EVIDENCE" {
		t.Fatal(r)
	}
	ev := e.evidence(t, reserve, false, false)
	release := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout, Release: &application.Release{Ref: ref, Reason: "FINAL_FAILURE", Evidence: &ev}}
	if r := e.execute(t, release); r.Reason != "INVALID_EVIDENCE" {
		t.Fatal(r)
	}
	if b := e.balance(t, f.WalletA); b.Held.String() != "20200" {
		t.Fatal(b)
	}
	ev = e.evidence(t, reserve, false, true)
	release.OperationID = domain.NewID()
	release.Release.Evidence = &ev
	if r := e.execute(t, release); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	b := e.balance(t, f.WalletA)
	a, _ := b.Available(domain.Wallet)
	if a.String() != "100000" || !b.Held.IsZero() {
		t.Fatal(b)
	}
	late := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.CapturePayout, Capture: &application.Capture{Ref: ref, Evidence: e.evidence(t, reserve, true, false)}}
	if r := e.execute(t, late); r.Reason != "STATE_CONFLICT" {
		t.Fatal(r)
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, f.BookID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counts["resolution_evidence"] != 2 {
		t.Fatal("late contradictory evidence not retained", snapshot.Counts)
	}
}
func TestCancelExposureRace(t *testing.T) {
	e := setup(t)
	reserve := e.reserve(t)
	if r := e.execute(t, reserve); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	expose := e.expose(reserve)
	cancel := application.Command{BookID: reserve.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout, Release: &application.Release{Ref: expose.Expose.Ref, Reason: "CANCEL"}}
	start := make(chan struct{})
	type answer struct {
		r   application.Receipt
		err error
	}
	answers := make(chan answer, 2)
	for _, cmd := range []application.Command{expose, cancel} {
		go func() {
			<-start
			r, err := e.service.Execute(context.Background(), e.caller, cmd)
			answers <- answer{r, err}
		}()
	}
	close(start)
	applied := 0
	var winner application.Kind
	for range 2 {
		a := <-answers
		if a.err != nil {
			t.Fatal(a.err)
		}
		if a.r.Outcome == "APPLIED" {
			applied++
			winner = a.r.Kind
		}
	}
	if applied != 1 {
		t.Fatal(applied)
	}
	held := e.balance(t, e.fixture.WalletA).Held.String()
	if (winner == application.MarkHoldExposed && held != "20200") || (winner == application.ReleasePayout && held != "0") {
		t.Fatal(winner, held)
	}
}
func TestDurableDeclineAfterControlChange(t *testing.T) {
	e := setup(t)
	f := e.fixture
	cap, _ := domain.ParseWide("200000")
	freeze := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.SetAccountControl, Control: &application.SetControl{Key: application.ControlKey{Kind: "SUBJECT", ID: f.OwnerA}, ExpectedVersion: 1, DailyCap: cap, DebitFrozen: true, Reason: "TEST"}}
	if r := e.execute(t, freeze); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	cmd := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	if r := e.execute(t, cmd); r.Reason != "ACCOUNT_BLOCKED" {
		t.Fatal(r)
	}
	freeze.OperationID = domain.NewID()
	freeze.Control.ExpectedVersion = 2
	freeze.Control.DebitFrozen = false
	if r := e.execute(t, freeze); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
	if r := e.execute(t, cmd); r.Reason != "ACCOUNT_BLOCKED" {
		t.Fatal(r)
	}
	cmd.OperationID = domain.NewID()
	if r := e.execute(t, cmd); r.Reason != "BUSINESS_ALREADY_DECIDED" {
		t.Fatal(r)
	}
	cmd.OperationID = domain.NewID()
	cmd.Transfer = &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}
	if r := e.execute(t, cmd); r.Outcome != "APPLIED" {
		t.Fatal(r)
	}
}

func setup(t *testing.T) *environment {
	t.Helper()
	dsn := os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL")
	runtimeDSN := os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL")
	if dsn == "" || runtimeDSN == "" {
		t.Skip("real CockroachDB integration test: set both PESARO_LEDGER_TEST_*_URL variables")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := cockroach.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err = admin.Ready(ctx); err != nil {
		t.Fatalf("prepare schema once with ledger-admin before parallel tests: %v", err)
	}
	store, err := cockroach.Open(ctx, runtimeDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	f, err := admin.BootstrapSynthetic(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gpub, gkey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	epub, ekey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	svc := application.New(store, application.Trust{GrantKeys: map[string]ed25519.PublicKey{"test-grant": gpub}, EvidenceKeys: map[string]ed25519.PublicKey{"test-evidence": epub}}, nil)
	caller := application.Caller{Identity: "test-payments", Books: map[domain.ID]bool{f.BookID: true}, Permissions: map[string]bool{"spend": true, "resolve": true, "control": true, "provision": true, "read": true, "verify": true}}
	return &environment{admin, store, f, svc, caller, gkey, ekey}
}
func amt(t *testing.T, n string) domain.Amount {
	t.Helper()
	a, e := domain.ParseAmount(n)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func (e *environment) terms(t *testing.T, principal, fee string, policy domain.ID) application.Terms {
	t.Helper()
	f := e.fixture
	now := time.Now().UTC()
	terms := application.Terms{PaymentID: domain.NewID(), SourceID: f.WalletA, BeneficiaryID: f.WalletB, Principal: amt(t, principal), Fee: amt(t, fee), Currency: "KES", PolicyID: policy, QuoteID: domain.NewID()}
	g := application.Grant{ID: domain.NewID(), Issuer: "test-grant", BookID: f.BookID, PaymentID: terms.PaymentID, SubjectID: f.OwnerA, SourceID: terms.SourceID, BeneficiaryID: terms.BeneficiaryID, Principal: terms.Principal, Fee: terms.Fee, PolicyID: terms.PolicyID, QuoteID: terms.QuoteID, Currency: "KES", SubjectEpoch: 1, AccountEpoch: 1, NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	var err error
	terms.Grant, err = application.SignGrant(g, e.grantKey)
	if err != nil {
		t.Fatal(err)
	}
	return terms
}
func (e *environment) execute(t *testing.T, c application.Command) application.Receipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := e.service.Execute(ctx, e.caller, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (e *environment) balance(t *testing.T, id domain.ID) domain.Totals {
	t.Helper()
	_, b, err := e.service.GetBalance(context.Background(), e.caller, e.fixture.BookID, id)
	if err != nil {
		t.Fatal(err)
	}
	return b.Totals
}
func TestDurableTransferGolden(t *testing.T) {
	e := setup(t)
	f := e.fixture
	cmd := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	r := e.execute(t, cmd)
	if r.Outcome != "APPLIED" {
		t.Fatalf("%+v", r)
	}
	a, err := e.balance(t, f.WalletA).Available(domain.Wallet)
	if err != nil || a.String() != "89900" {
		t.Fatal(a, err)
	}
	b, _ := e.balance(t, f.WalletB).Posted(domain.Wallet)
	if b.String() != "10000" {
		t.Fatal(b)
	}
	fee, _ := e.balance(t, f.FeeAccount).Posted(domain.FeeIncome)
	if fee.String() != "100" {
		t.Fatal(fee)
	}
	replay := e.execute(t, cmd)
	if replay.JournalID != r.JournalID || replay.EventID != r.EventID {
		t.Fatal("duplicate effect")
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, f.BookID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Counts["journals"] != 2 || snapshot.Counts["financial_operations"] != 2 || snapshot.Counts["outbox_facts"] != 2 {
		t.Fatal(snapshot.Counts)
	}
}
