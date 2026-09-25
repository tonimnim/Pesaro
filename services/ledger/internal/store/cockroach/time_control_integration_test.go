package cockroach_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func parallelCommands(t *testing.T, e *environment, commands ...application.Command) []application.Receipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start, done := make(chan struct{}), make(chan int, len(commands))
	answers := make([]referenceAnswer, len(commands))
	for i, cmd := range commands {
		go func() {
			<-start
			answers[i].Receipt, answers[i].Error = e.service.Execute(ctx, e.caller, cmd)
			done <- i
		}()
	}
	close(start)
	for range commands {
		<-done
	}
	result := make([]application.Receipt, len(commands))
	for i, answer := range answers {
		if answer.Error != nil {
			t.Fatal(answer.Error)
		}
		result[i] = answer.Receipt
	}
	return result
}
func requireApplied(t *testing.T, r application.Receipt) {
	t.Helper()
	if r.Outcome != "APPLIED" {
		t.Fatalf("unexpected rejection: %s", r.Reason)
	}
}
func (e *environment) setSubject(t *testing.T, owner domain.ID, version, cap int64, freeze, revoke bool) application.Command {
	t.Helper()
	limit, err := domain.ParseWide(strconv.FormatInt(cap, 10))
	if err != nil {
		t.Fatal(err)
	}
	return application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.SetAccountControl,
		Control: &application.SetControl{Key: application.ControlKey{Kind: "SUBJECT", ID: owner}, ExpectedVersion: version, DailyCap: limit, DebitFrozen: freeze, RevokeGrants: revoke, Reason: "TEST_CONTROL"}}
}

func TestOriginalLimitBucketsSurviveMidnightAndLoweredCap(t *testing.T) {
	e := setup(t)
	// Nairobi is UTC+3. Keep the oracle date calculation independent of Bucket.
	now := time.Date(2026, 9, 25, 20, 59, 0, 0, time.UTC)
	fixedClock(e, &now)
	reserves := map[string]application.Command{}
	makeCommand := func(kind string, principal int64) application.Command {
		input := referenceInstruction{Key: string(domain.NewID()), Payment: string(domain.NewID()), Hold: string(domain.NewID()), Kind: kind, Principal: principal, Fee: 100}
		return e.referenceCase(t, input, now, reserves).Command
	}
	captureReservation, releaseReservation := makeCommand("reserve", 20000), makeCommand("reserve", 15000)
	requireApplied(t, e.execute(t, captureReservation))
	requireApplied(t, e.execute(t, releaseReservation))
	requireApplied(t, e.execute(t, e.expose(captureReservation)))
	now = now.Add(3 * time.Minute)
	requireApplied(t, e.execute(t, e.setSubject(t, e.fixture.OwnerA, 1, 10000, false, false)))
	ref := e.expose(captureReservation).Expose.Ref
	ref.ExpectedVersion = 2
	capture := application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.CapturePayout,
		Capture: &application.Capture{Ref: ref, Evidence: e.evidenceAt(t, captureReservation, true, false, now)}}
	requireApplied(t, e.execute(t, capture))
	cancel := application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout,
		Release: &application.Release{Ref: e.expose(releaseReservation).Expose.Ref, Reason: "CANCEL"}}
	requireApplied(t, e.execute(t, cancel))
	requireApplied(t, e.execute(t, makeCommand("transfer", 9000)))
	if r := e.execute(t, makeCommand("transfer", 2000)); r.Reason != "LIMIT_EXCEEDED" {
		t.Fatal(r)
	}
	// A clock rollback must still see the old bucket's consumed usage, which is
	// already above the newly lowered cap. It cannot manufacture fresh capacity.
	now = now.Add(-4 * time.Minute)
	if r := e.execute(t, makeCommand("transfer", 1)); r.Reason != "LIMIT_EXCEEDED" {
		t.Fatal(r)
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"2026-09-25": "20000", "2026-09-26": "9000"}
	for _, row := range snapshot.Tables["limit_usage"] {
		bucket := row["bucket"].(string)
		if row["owner_id"] != string(e.fixture.OwnerA) || row["reserved"] != "0" || row["consumed"] != want[bucket] {
			t.Fatalf("wrong original bucket allocation: %+v", row)
		}
		delete(want, bucket)
	}
	if len(want) != 0 {
		t.Fatal("missing usage bucket", want)
	}
	verifyReferenceSnapshot(t, "midnight-lowered-cap", snapshot)
}

func TestExpiryExposureAndExpiredGrantReplay(t *testing.T) {
	e := setup(t)
	now := time.Now().UTC().Truncate(time.Second)
	fixedClock(e, &now)
	reserves := map[string]application.Command{}
	makeReserve := func() application.Command {
		input := referenceInstruction{Key: string(domain.NewID()), Payment: string(domain.NewID()), Hold: string(domain.NewID()), Kind: "reserve", Principal: 20000, Fee: 200}
		return e.referenceCase(t, input, now, reserves).Command
	}
	exposed, expires := makeReserve(), makeReserve()
	original := e.execute(t, exposed)
	requireApplied(t, original)
	requireApplied(t, e.execute(t, expires))
	requireApplied(t, e.execute(t, e.expose(exposed)))
	newSpend := e.referenceCase(t, referenceInstruction{Key: string(domain.NewID()), Payment: string(domain.NewID()), Kind: "transfer", Principal: 100, Fee: 0}, now, reserves).Command
	now = expires.Reserve.ExpiresAt
	lateExposure := e.expose(expires)
	expiry := application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout, Release: &application.Release{Ref: lateExposure.Expose.Ref, Reason: "EXPIRY"}}
	results := parallelCommands(t, e, lateExposure, expiry)
	if results[0].Outcome != "REJECTED" || results[1].Outcome != "APPLIED" {
		t.Fatal("expired reservation exposed", results)
	}
	ref := e.expose(exposed).Expose.Ref
	ref.ExpectedVersion = 2
	exposedExpiry := application.Command{BookID: e.fixture.BookID, OperationID: domain.NewID(), Kind: application.ReleasePayout, Release: &application.Release{Ref: ref, Reason: "EXPIRY"}}
	if r := e.execute(t, exposedExpiry); r.Reason != "EXPOSED_REQUIRES_EVIDENCE" {
		t.Fatal(r)
	}
	now = now.Add(2 * time.Hour)
	if replay := e.execute(t, exposed); !reflect.DeepEqual(replay, original) {
		t.Fatal("durable result changed after grant expiry")
	}
	if r := e.execute(t, newSpend); r.Reason != "GRANT_EXPIRED" {
		t.Fatal(r)
	}
	now = now.Add(-3 * time.Hour)
	lateExposure.OperationID = domain.NewID()
	if r := e.execute(t, lateExposure); r.Outcome != "REJECTED" {
		t.Fatal("clock rollback reopened a released hold")
	}
	if e.balance(t, e.fixture.WalletA).Held.String() != "20200" || e.balance(t, e.fixture.Pool).Held.String() != "20000" {
		t.Fatal("exposed reservations lost")
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, e.fixture.BookID)
	if err != nil {
		t.Fatal(err)
	}
	verifyReferenceSnapshot(t, "expiry-and-clock-rollback", snapshot)
}

func TestSharedOwnerLimitAcrossWallets(t *testing.T) {
	e := setup(t)
	f := e.fixture
	second := domain.NewID()
	create := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.CreateAccount, Create: &application.Create{AccountID: second, OwnerID: f.OwnerA, Purpose: domain.Wallet}}
	requireApplied(t, e.execute(t, create))
	terms := e.terms(t, "50000", "0", f.PolicyZero)
	terms.BeneficiaryID, terms.Grant.Claims.BeneficiaryID = second, second
	var err error
	terms.Grant, err = application.SignGrant(terms.Grant.Claims, e.grantKey)
	if err != nil {
		t.Fatal(err)
	}
	requireApplied(t, e.execute(t, application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: terms}}))
	requireApplied(t, e.execute(t, e.setSubject(t, f.OwnerA, 1, 110000, false, false)))
	commands := []application.Command{}
	for _, source := range []domain.ID{f.WalletA, second} {
		terms := e.terms(t, "40000", "1000", f.Policy1000)
		terms.SourceID, terms.Grant.Claims.SourceID = source, source
		terms.Grant, err = application.SignGrant(terms.Grant.Claims, e.grantKey)
		if err != nil {
			t.Fatal(err)
		}
		commands = append(commands, application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: terms}})
	}
	applied, limited := 0, 0
	for _, r := range parallelCommands(t, e, commands...) {
		if r.Outcome == "APPLIED" {
			applied++
		} else if r.Reason == "LIMIT_EXCEEDED" {
			limited++
		}
	}
	if applied != 1 || limited != 1 {
		t.Fatal("shared subject capacity reused", applied, limited)
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, f.BookID)
	if err != nil {
		t.Fatal(err)
	}
	if rows := snapshot.Tables["limit_usage"]; len(rows) != 1 || rows[0]["consumed"] != "90000" || rows[0]["reserved"] != "0" {
		t.Fatal("subject usage", rows)
	}
	verifyReferenceSnapshot(t, "shared-owner-limit", snapshot)
}

func TestFreezeAcknowledgementOrdersSpending(t *testing.T) {
	e := setup(t)
	f := e.fixture
	spend := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	freeze := e.setSubject(t, f.OwnerA, 1, 200000, true, true)
	results := parallelCommands(t, e, spend, freeze)
	requireApplied(t, results[1])
	if results[0].Outcome != "APPLIED" && results[0].Reason != "AUTHORIZATION_REVOKED" {
		t.Fatal(results[0])
	}
	// After acknowledgement, a freshly signed grant for the current epoch is
	// still unable to spend while the authoritative debit freeze is effective.
	after := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	after.Transfer.Terms.Grant.Claims.SubjectEpoch = 2
	var err error
	after.Transfer.Terms.Grant, err = application.SignGrant(after.Transfer.Terms.Grant.Claims, e.grantKey)
	if err != nil {
		t.Fatal(err)
	}
	if r := e.execute(t, after); r.Reason != "ACCOUNT_BLOCKED" {
		t.Fatal(r)
	}
	requireApplied(t, e.execute(t, e.setSubject(t, f.OwnerA, 2, 200000, false, false)))
	stale := application.Command{BookID: f.BookID, OperationID: domain.NewID(), Kind: application.TransferInternal, Transfer: &application.Transfer{Terms: e.terms(t, "10000", "100", f.Policy100)}}
	if r := e.execute(t, stale); r.Reason != "AUTHORIZATION_REVOKED" {
		t.Fatal(r)
	}
	snapshot, err := e.service.Export(context.Background(), e.caller, f.BookID)
	if err != nil {
		t.Fatal(err)
	}
	verifyReferenceSnapshot(t, "freeze-ordering", snapshot)
}
