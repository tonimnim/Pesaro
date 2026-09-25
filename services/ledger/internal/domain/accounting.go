package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIdentity = errors.New("invalid nonzero UUID")
	ErrBook     = errors.New("unsupported book or currency")
	ErrAccount  = errors.New("invalid account purpose or ownership")
	ErrJournal  = errors.New("invalid or unbalanced journal")
)

type ID string

func ParseID(s string) (ID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return "", ErrIdentity
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(raw) != 16 {
		return "", ErrIdentity
	}
	nonzero := false
	for _, b := range raw {
		nonzero = nonzero || b != 0
	}
	if !nonzero {
		return "", ErrIdentity
	}
	return ID(strings.ToLower(s)), nil
}
func NewID() ID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return ID(fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]))
}
func (id ID) Valid() bool { parsed, err := ParseID(string(id)); return err == nil && parsed == id }

type Book struct {
	ID       ID
	Currency string
	Scale    int
}

func (b Book) Validate() error {
	if !b.ID.Valid() || b.Currency != "KES" || b.Scale != 2 {
		return ErrBook
	}
	return nil
}

type Purpose string

const (
	Wallet       Purpose = "WALLET"
	ProviderPool Purpose = "PROVIDER_POOL"
	FeeIncome    Purpose = "FEE_INCOME"
)

type Side string

const (
	Debit  Side = "DEBIT"
	Credit Side = "CREDIT"
)

type Account struct {
	ID      ID
	Book    Book
	Owner   ID
	Purpose Purpose
}

func (a Account) Validate() error {
	if !a.ID.Valid() || !a.Owner.Valid() || a.Book.Validate() != nil {
		return ErrAccount
	}
	if a.Purpose != Wallet && a.Purpose != ProviderPool && a.Purpose != FeeIncome {
		return ErrAccount
	}
	return nil
}
func (a Account) NormalSide() Side {
	if a.Purpose == ProviderPool {
		return Debit
	}
	return Credit
}

type Totals struct {
	Debits  Wide
	Credits Wide
	Held    Wide
}

func (t Totals) Posted(p Purpose) (Wide, error) {
	switch p {
	case ProviderPool:
		return t.Debits.Sub(t.Credits)
	case Wallet, FeeIncome:
		return t.Credits.Sub(t.Debits)
	default:
		return Wide{}, ErrAccount
	}
}
func (t Totals) Available(p Purpose) (Wide, error) {
	posted, err := t.Posted(p)
	if err != nil {
		return Wide{}, err
	}
	return posted.Sub(t.Held)
}
func (t Totals) Post(p Purpose, side Side, amount Amount) (Totals, error) {
	next := t
	var err error
	switch side {
	case Debit:
		next.Debits, err = t.Debits.Add(WideAmount(amount))
	case Credit:
		next.Credits, err = t.Credits.Add(WideAmount(amount))
	default:
		return t, ErrJournal
	}
	if err != nil {
		return t, err
	}
	if _, err = next.Available(p); err != nil {
		return t, err
	}
	return next, nil
}
func (t Totals) Reserve(p Purpose, amount Amount) (Totals, error) {
	if p != Wallet && p != ProviderPool {
		return t, ErrAccount
	}
	next := t
	var err error
	next.Held, err = t.Held.Add(WideAmount(amount))
	if err != nil {
		return t, err
	}
	if _, err = next.Available(p); err != nil {
		return t, err
	}
	return next, nil
}
func (t Totals) Release(amount Amount) (Totals, error) {
	next := t
	var err error
	next.Held, err = t.Held.Sub(WideAmount(amount))
	if err != nil {
		return t, err
	}
	return next, nil
}

type Line struct {
	Account Account
	Side    Side
	Amount  Amount
}
type Journal struct {
	book   Book
	lines  []Line
	digest string
}

func NewJournal(book Book, lines []Line) (Journal, error) {
	if book.Validate() != nil || len(lines) < 2 || len(lines) > 8 {
		return Journal{}, ErrJournal
	}
	var debits, credits Wide
	seen := map[ID]bool{}
	hash := sha256.New()
	fmt.Fprintf(hash, "%s|%s|%d\n", book.ID, book.Currency, book.Scale)
	for _, line := range lines {
		if line.Account.Validate() != nil || line.Account.Book != book || line.Amount.Principal() != nil || seen[line.Account.ID] {
			return Journal{}, ErrJournal
		}
		seen[line.Account.ID] = true
		var err error
		switch line.Side {
		case Debit:
			debits, err = debits.Add(WideAmount(line.Amount))
		case Credit:
			credits, err = credits.Add(WideAmount(line.Amount))
		default:
			return Journal{}, ErrJournal
		}
		if err != nil {
			return Journal{}, err
		}
		fmt.Fprintf(hash, "%s|%s|%s\n", line.Account.ID, line.Side, line.Amount.String())
	}
	if debits.Cmp(credits) != 0 {
		return Journal{}, ErrJournal
	}
	return Journal{book, append([]Line(nil), lines...), hex.EncodeToString(hash.Sum(nil))}, nil
}
func (j Journal) Book() Book     { return j.book }
func (j Journal) Lines() []Line  { return append([]Line(nil), j.lines...) }
func (j Journal) Digest() string { return j.digest }
