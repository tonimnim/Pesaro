package domain

import (
	"errors"
	"math"
	"time"
)

var (
	ErrState    = errors.New("incompatible state or version")
	ErrExpired  = errors.New("reservation expired")
	ErrEvidence = errors.New("final evidence required")
	ErrLimit    = errors.New("outgoing limit exceeded")
)

type HoldState string

const (
	Reserved HoldState = "RESERVED"
	Exposed  HoldState = "EXPOSED"
	Captured HoldState = "CAPTURED"
	Released HoldState = "RELEASED"
)

// HoldStateValue contains only transition state. Immutable financial terms live
// beside it and cannot be replaced by a resolution command.
type HoldStateValue struct {
	State     HoldState
	Version   int64
	ExpiresAt time.Time
}

func NextVersion(current int64) (int64, error) {
	if current < 0 || current == math.MaxInt64 {
		return 0, ErrOverflow
	}
	return current + 1, nil
}
func (h HoldStateValue) Expose(expected int64, now time.Time) (HoldStateValue, error) {
	if h.State != Reserved || h.Version != expected {
		return h, ErrState
	}
	if !now.Before(h.ExpiresAt) {
		return h, ErrExpired
	}
	v, e := NextVersion(h.Version)
	if e != nil {
		return h, e
	}
	h.State = Exposed
	h.Version = v
	return h, nil
}
func (h HoldStateValue) Capture(expected int64, verifiedSuccess bool) (HoldStateValue, error) {
	if h.State != Exposed || h.Version != expected {
		return h, ErrState
	}
	if !verifiedSuccess {
		return h, ErrEvidence
	}
	v, e := NextVersion(h.Version)
	if e != nil {
		return h, e
	}
	h.State = Captured
	h.Version = v
	return h, nil
}
func (h HoldStateValue) Release(expected int64, verifiedFencedFailure bool) (HoldStateValue, error) {
	if h.Version != expected || (h.State != Reserved && h.State != Exposed) {
		return h, ErrState
	}
	if h.State == Exposed && !verifiedFencedFailure {
		return h, ErrEvidence
	}
	v, e := NextVersion(h.Version)
	if e != nil {
		return h, e
	}
	h.State = Released
	h.Version = v
	return h, nil
}

type Usage struct {
	Reserved Wide
	Consumed Wide
}

func (u Usage) Admit(cap Wide, amount Amount, reserve bool) (Usage, error) {
	total, e := u.Reserved.Add(u.Consumed)
	if e != nil {
		return u, e
	}
	total, e = total.Add(WideAmount(amount))
	if e != nil {
		return u, e
	}
	if total.Cmp(cap) > 0 {
		return u, ErrLimit
	}
	next := u
	if reserve {
		next.Reserved, e = u.Reserved.Add(WideAmount(amount))
	} else {
		next.Consumed, e = u.Consumed.Add(WideAmount(amount))
	}
	if e != nil {
		return u, e
	}
	return next, nil
}
func (u Usage) Resolve(amount Amount, capture bool) (Usage, error) {
	next := u
	var e error
	next.Reserved, e = u.Reserved.Sub(WideAmount(amount))
	if e != nil {
		return u, e
	}
	if capture {
		next.Consumed, e = u.Consumed.Add(WideAmount(amount))
		if e != nil {
			return u, e
		}
	}
	return next, nil
}
