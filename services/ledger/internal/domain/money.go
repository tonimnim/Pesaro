// Package domain owns Ledger's exact values and financial invariants.
package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strconv"
)

var (
	ErrAmount    = errors.New("invalid canonical minor-unit amount")
	ErrOverflow  = errors.New("financial value exceeds bound")
	ErrUnderflow = errors.New("financial value would be negative")
)

// Amount is an immutable nonnegative int64 command amount. The zero value is zero.
type Amount struct{ units int64 }

func NewAmount(n int64) (Amount, error) {
	if n < 0 {
		return Amount{}, ErrAmount
	}
	return Amount{n}, nil
}

func ParseAmount(s string) (Amount, error) {
	if !canonicalDigits(s, 19) {
		return Amount{}, ErrAmount
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return Amount{}, ErrOverflow
	}
	return Amount{n}, nil
}

func (a Amount) String() string { return strconv.FormatInt(a.units, 10) }
func (a Amount) Int64() int64   { return a.units }
func (a Amount) IsZero() bool   { return a.units == 0 }
func (a Amount) Principal() error {
	if a.units <= 0 {
		return ErrAmount
	}
	return nil
}
func (a Amount) Add(b Amount) (Amount, error) {
	if a.units > math.MaxInt64-b.units {
		return Amount{}, ErrOverflow
	}
	return Amount{a.units + b.units}, nil
}
func (a Amount) Sub(b Amount) (Amount, error) {
	if a.units < b.units {
		return Amount{}, ErrUnderflow
	}
	return Amount{a.units - b.units}, nil
}
func (a Amount) MarshalJSON() ([]byte, error) { return json.Marshal(a.String()) }
func (a *Amount) UnmarshalJSON(data []byte) error {
	s, err := exactJSONString(data)
	if err != nil {
		return err
	}
	value, err := ParseAmount(s)
	if err == nil {
		*a = value
	}
	return err
}

// Wide stores a canonical immutable value in [0, 10^38-1]. Strings prevent
// mutable big.Int aliasing; arithmetic uses fresh local big.Int values only.
type Wide struct{ digits string }

func ParseWide(s string) (Wide, error) {
	if !canonicalDigits(s, 38) {
		return Wide{}, ErrAmount
	}
	if s == "0" {
		return Wide{}, nil
	}
	return Wide{s}, nil
}
func WideAmount(a Amount) Wide { w, _ := ParseWide(a.String()); return w }
func (w Wide) String() string {
	if w.digits == "" {
		return "0"
	}
	return w.digits
}
func (w Wide) IsZero() bool { return w.digits == "" }
func (w Wide) Cmp(other Wide) int {
	a, b := w.String(), other.String()
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func (w Wide) Add(other Wide) (Wide, error) { return w.calculate(other, false) }
func (w Wide) Sub(other Wide) (Wide, error) { return w.calculate(other, true) }
func (w Wide) calculate(other Wide, subtract bool) (Wide, error) {
	a, _ := new(big.Int).SetString(w.String(), 10)
	b, _ := new(big.Int).SetString(other.String(), 10)
	if subtract {
		a.Sub(a, b)
	} else {
		a.Add(a, b)
	}
	if a.Sign() < 0 {
		return Wide{}, ErrUnderflow
	}
	if len(a.String()) > 38 {
		return Wide{}, ErrOverflow
	}
	return ParseWide(a.String())
}
func (w Wide) MarshalJSON() ([]byte, error) { return json.Marshal(w.String()) }
func (w *Wide) UnmarshalJSON(data []byte) error {
	s, err := exactJSONString(data)
	if err != nil {
		return err
	}
	value, err := ParseWide(s)
	if err == nil {
		*w = value
	}
	return err
}

func canonicalDigits(s string, max int) bool {
	if s == "" || len(s) > max || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func exactJSONString(data []byte) (string, error) {
	// Reject null and JSON numbers, including zero. No float conversion occurs.
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return "", ErrAmount
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return "", ErrAmount
	}
	// Financial encodings must be the literal canonical ASCII digits, not escapes.
	encoded, _ := json.Marshal(s)
	if !bytes.Equal(data, encoded) {
		return "", ErrAmount
	}
	return s, nil
}
