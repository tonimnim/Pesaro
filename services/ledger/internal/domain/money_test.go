package domain_test

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func amount(t *testing.T, s string) domain.Amount {
	t.Helper()
	a, e := domain.ParseAmount(s)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func wide(t *testing.T, s string) domain.Wide {
	t.Helper()
	a, e := domain.ParseWide(s)
	if e != nil {
		t.Fatal(e)
	}
	return a
}

func TestAmountBoundaries(t *testing.T) {
	for _, s := range []string{"", "-1", "+1", "00", "01", " 1", "1 ", "1.0", "1e3", "NaN", "Infinity", "١", "9223372036854775808"} {
		if _, err := domain.ParseAmount(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
	max := amount(t, "9223372036854775807")
	if _, err := max.Add(amount(t, "1")); !errors.Is(err, domain.ErrOverflow) {
		t.Fatal(err)
	}
	if _, err := amount(t, "0").Sub(amount(t, "1")); !errors.Is(err, domain.ErrUnderflow) {
		t.Fatal(err)
	}
	if _, err := domain.NewAmount(-1); err == nil {
		t.Fatal("negative")
	}
	if err := (domain.Amount{}).Principal(); err == nil {
		t.Fatal("zero principal")
	}
	total, err := amount(t, "20000").Add(amount(t, "200"))
	if err != nil || total.String() != "20200" {
		t.Fatal(total, err)
	}
	diff, err := total.Sub(amount(t, "200"))
	if err != nil || diff.String() != "20000" {
		t.Fatal(diff, err)
	}
}

func TestCanonicalJSONAndNoMutationOnFailure(t *testing.T) {
	for _, s := range []string{`null`, `0`, `"01"`, `"\u0031"`, `"1.0"`, `"9223372036854775808"`} {
		a := amount(t, "123")
		if json.Unmarshal([]byte(s), &a) == nil {
			t.Fatalf("accepted %s", s)
		}
		if a.String() != "123" {
			t.Fatal("failed decode changed amount")
		}
	}
	a := amount(t, "9223372036854775807")
	b, e := json.Marshal(a)
	if e != nil || string(b) != `"9223372036854775807"` {
		t.Fatal(string(b), e)
	}
	var decoded domain.Amount
	if err := json.Unmarshal(b, &decoded); err != nil || decoded != a {
		t.Fatal(decoded, err)
	}
}

func TestWideExactAndImmutable(t *testing.T) {
	max := wide(t, "99999999999999999999999999999999999999")
	copy := max
	if _, err := max.Add(wide(t, "1")); !errors.Is(err, domain.ErrOverflow) {
		t.Fatal(err)
	}
	if max != copy {
		t.Fatal("operand mutated")
	}
	if _, err := (domain.Wide{}).Sub(wide(t, "1")); !errors.Is(err, domain.ErrUnderflow) {
		t.Fatal(err)
	}
	a := domain.WideAmount(amount(t, "9223372036854775807"))
	sum, err := a.Add(a)
	if err != nil || sum.String() != "18446744073709551614" {
		t.Fatal(sum, err)
	}
	for _, s := range []string{"-1", "01", "1.0", "NaN", "100000000000000000000000000000000000000"} {
		if _, err := domain.ParseWide(s); err == nil {
			t.Fatal(s)
		}
	}
	for _, s := range []string{"0", "1", "18446744073709551614", max.String()} {
		v := wide(t, s)
		data, _ := json.Marshal(v)
		var back domain.Wide
		if err := json.Unmarshal(data, &back); err != nil || v != back {
			t.Fatal(s, err)
		}
	}
}

func FuzzAmountArithmetic(f *testing.F) {
	f.Add(int64(100000), int64(20200))
	f.Add(int64(math.MaxInt64), int64(1))
	f.Add(int64(0), int64(0))
	f.Fuzz(func(t *testing.T, x, y int64) {
		if x < 0 || y < 0 {
			return
		}
		a, _ := domain.NewAmount(x)
		b, _ := domain.NewAmount(y)
		want := new(big.Int).Add(big.NewInt(x), big.NewInt(y))
		got, err := a.Add(b)
		if want.IsInt64() {
			if err != nil || got.String() != want.String() {
				t.Fatal(x, y, got, err)
			}
		} else if !errors.Is(err, domain.ErrOverflow) {
			t.Fatal("missed overflow")
		}
		diff, err := a.Sub(b)
		if x >= y {
			if err != nil || diff.Int64() != x-y {
				t.Fatal("subtraction")
			}
		} else if !errors.Is(err, domain.ErrUnderflow) {
			t.Fatal("missed underflow")
		}
	})
}
