package domain_test

import (
	"fmt"
	"testing"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func id(n int) domain.ID { return domain.ID(fmt.Sprintf("00000000-0000-4000-8000-%012d", n)) }
func book() domain.Book  { return domain.Book{ID: id(1), Currency: "KES", Scale: 2} }
func account(n int, p domain.Purpose) domain.Account {
	return domain.Account{ID: id(n), Book: book(), Owner: id(100 + n), Purpose: p}
}

func TestIdentityAndScope(t *testing.T) {
	for _, s := range []string{"", "00000000-0000-0000-0000-000000000000", "00000000000040008000000000000001", "gggggggg-0000-4000-8000-000000000001"} {
		if _, e := domain.ParseID(s); e == nil {
			t.Fatal(s)
		}
	}
	for range 20 {
		if !domain.NewID().Valid() {
			t.Fatal("new ID")
		}
	}
	v, e := domain.ParseID("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")
	if e != nil || string(v) != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Fatal(v, e)
	}
	for _, b := range []domain.Book{{ID: id(1), Currency: "USD", Scale: 2}, {ID: id(1), Currency: "KES", Scale: 0}, {Currency: "KES", Scale: 2}} {
		if b.Validate() == nil {
			t.Fatal(b)
		}
	}
	bad := account(2, "ARBITRARY")
	if bad.Validate() == nil {
		t.Fatal("purpose")
	}
}

func TestJournalWideSumsAndImmutability(t *testing.T) {
	max := amount(t, "9223372036854775807")
	lines := []domain.Line{{Account: account(2, domain.Wallet), Side: domain.Debit, Amount: max}, {Account: account(3, domain.Wallet), Side: domain.Debit, Amount: max}, {Account: account(4, domain.Wallet), Side: domain.Credit, Amount: max}, {Account: account(5, domain.Wallet), Side: domain.Credit, Amount: max}}
	j, e := domain.NewJournal(book(), lines)
	if e != nil {
		t.Fatal(e)
	}
	digest := j.Digest()
	lines[0].Amount = amount(t, "1")
	got := j.Lines()
	got[1].Amount = amount(t, "1")
	if j.Lines()[0].Amount != max || j.Lines()[1].Amount != max || j.Digest() != digest {
		t.Fatal("mutable journal")
	}
	for _, mutate := range []func([]domain.Line){
		func(v []domain.Line) { v[0].Amount = amount(t, "1") },
		func(v []domain.Line) { v[0].Amount = amount(t, "0") },
		func(v []domain.Line) { v[0].Account.Book.ID = id(9) },
		func(v []domain.Line) { v[0].Side = "WRONG" },
		func(v []domain.Line) { v[0].Account.ID = v[1].Account.ID },
	} {
		v := j.Lines()
		mutate(v)
		if _, e := domain.NewJournal(book(), v); e == nil {
			t.Fatal("invalid journal accepted")
		}
	}
}

func TestGoldenBalancesAndReservations(t *testing.T) {
	wallet := domain.Totals{Credits: wide(t, "100000")}
	pool := domain.Totals{Debits: wide(t, "100000")}
	reserved, e := wallet.Reserve(domain.Wallet, amount(t, "20200"))
	if e != nil {
		t.Fatal(e)
	}
	available, e := reserved.Available(domain.Wallet)
	if e != nil || available.String() != "79800" {
		t.Fatal(available, e)
	}
	if _, e := reserved.Post(domain.Wallet, domain.Debit, amount(t, "80000")); e == nil {
		t.Fatal("spent held money")
	}
	asset, e := pool.Reserve(domain.ProviderPool, amount(t, "20000"))
	if e != nil {
		t.Fatal(e)
	}
	usable, e := asset.Available(domain.ProviderPool)
	if e != nil || usable.String() != "80000" {
		t.Fatal(usable, e)
	}
	released, e := reserved.Release(amount(t, "20200"))
	if e != nil || released != wallet {
		t.Fatal(released, e)
	}
	if _, e := released.Release(amount(t, "1")); e == nil {
		t.Fatal("double release")
	}
	captured, e := released.Post(domain.Wallet, domain.Debit, amount(t, "20200"))
	if e != nil {
		t.Fatal(e)
	}
	posted, _ := captured.Posted(domain.Wallet)
	if posted.String() != "79800" {
		t.Fatal(posted)
	}
	asset, e = asset.Release(amount(t, "20000"))
	if e != nil {
		t.Fatal(e)
	}
	asset, e = asset.Post(domain.ProviderPool, domain.Credit, amount(t, "20000"))
	if e != nil {
		t.Fatal(e)
	}
	usable, _ = asset.Available(domain.ProviderPool)
	if usable.String() != "80000" {
		t.Fatal(usable)
	}
	first, e := wallet.Post(domain.Wallet, domain.Debit, amount(t, "61000"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e := first.Post(domain.Wallet, domain.Debit, amount(t, "61000")); e == nil {
		t.Fatal("overspend")
	}
	available, _ = first.Available(domain.Wallet)
	if available.String() != "39000" {
		t.Fatal(available)
	}
	if _, e := wallet.Reserve(domain.FeeIncome, amount(t, "1")); e == nil {
		t.Fatal("income hold")
	}
}
