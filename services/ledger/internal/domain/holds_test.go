package domain_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func TestExposureStateMachine(t *testing.T) {
	now := time.Date(2026, 9, 25, 20, 59, 0, 0, time.UTC)
	reserved := domain.HoldStateValue{State: domain.Reserved, Version: 1, ExpiresAt: now.Add(time.Minute)}
	exposed, e := reserved.Expose(1, now)
	if e != nil || exposed.State != domain.Exposed || exposed.Version != 2 {
		t.Fatal(exposed, e)
	}
	if _, e := reserved.Expose(1, now.Add(time.Minute)); !errors.Is(e, domain.ErrExpired) {
		t.Fatal(e)
	}
	if _, e := reserved.Expose(2, now); !errors.Is(e, domain.ErrState) {
		t.Fatal(e)
	}
	if _, e := exposed.Release(2, false); !errors.Is(e, domain.ErrEvidence) {
		t.Fatal("timeout/cancel released exposure", e)
	}
	if _, e := exposed.Capture(2, false); !errors.Is(e, domain.ErrEvidence) {
		t.Fatal(e)
	}
	captured, e := exposed.Capture(2, true)
	if e != nil || captured.State != domain.Captured {
		t.Fatal(captured, e)
	}
	released, e := exposed.Release(2, true)
	if e != nil || released.State != domain.Released {
		t.Fatal(released, e)
	}
	for _, terminal := range []domain.HoldStateValue{captured, released} {
		if _, e := terminal.Capture(terminal.Version, true); e == nil {
			t.Fatal("terminal capture")
		}
		if _, e := terminal.Release(terminal.Version, true); e == nil {
			t.Fatal("terminal release")
		}
		if _, e := terminal.Expose(terminal.Version, now); e == nil {
			t.Fatal("reopen")
		}
	}
	cancelled, e := reserved.Release(1, false)
	if e != nil || cancelled.State != domain.Released {
		t.Fatal(cancelled, e)
	}
	if _, e := domain.NextVersion(math.MaxInt64); !errors.Is(e, domain.ErrOverflow) {
		t.Fatal(e)
	}
}

func TestLimitsPreserveOriginalAllocations(t *testing.T) {
	cap := wide(t, "25000")
	var u domain.Usage
	u, e := u.Admit(cap, amount(t, "20000"), true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := u.Admit(cap, amount(t, "6000"), false); !errors.Is(e, domain.ErrLimit) {
		t.Fatal(e)
	}
	// Lowering cap does not erase reservations; resolution uses no current cap.
	if _, e := u.Admit(wide(t, "10000"), amount(t, "1"), false); !errors.Is(e, domain.ErrLimit) {
		t.Fatal(e)
	}
	captured, e := u.Resolve(amount(t, "20000"), true)
	if e != nil || !captured.Reserved.IsZero() || captured.Consumed.String() != "20000" {
		t.Fatal(captured, e)
	}
	released, e := u.Resolve(amount(t, "20000"), false)
	if e != nil || !released.Reserved.IsZero() || !released.Consumed.IsZero() {
		t.Fatal(released, e)
	}
	if _, e := captured.Resolve(amount(t, "20000"), true); e == nil {
		t.Fatal("double limit consumption")
	}
}
