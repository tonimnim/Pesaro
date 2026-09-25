package cockroach

import (
	"context"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

// Fixture is synthetic bootstrap identity, not a customer or production funding API.
type Fixture struct {
	BookID          domain.ID
	WalletA         domain.ID
	WalletB         domain.ID
	Pool            domain.ID
	FeeAccount      domain.ID
	OwnerA          domain.ID
	OwnerB          domain.ID
	SystemOwner     domain.ID
	Policy100       domain.ID
	Policy200       domain.ID
	Policy1000      domain.ID
	PolicyZero      domain.ID
	ProviderAccount domain.ID
	Capability      domain.ID
	OperationID     domain.ID
}

func (s *Store) BootstrapSynthetic(ctx context.Context) (Fixture, error) {
	f := Fixture{BookID: domain.NewID(), WalletA: domain.NewID(), WalletB: domain.NewID(), Pool: domain.NewID(), FeeAccount: domain.NewID(), OwnerA: domain.NewID(), OwnerB: domain.NewID(), SystemOwner: domain.NewID(), Policy100: domain.NewID(), Policy200: domain.NewID(), Policy1000: domain.NewID(), PolicyZero: domain.NewID(), ProviderAccount: domain.NewID(), Capability: domain.NewID(), OperationID: domain.NewID()}
	cap, _ := domain.ParseWide("200000")
	funding, _ := domain.ParseAmount("100000")
	journalID, eventID := domain.NewID(), domain.NewID()
	_, err := s.Transact(ctx, func(transaction application.Transaction) (application.Receipt, error) {
		t := transaction.(*tx)
		// Runtime credentials do not have INSERT on books/policies. The bootstrap
		// tool needs separate administrative credentials and creates a fresh book.
		if err := t.write(ctx, "INSERT INTO books(book_id,entity_id,country,currency,scale,synthetic,default_cap) VALUES($1,$2,'KE','KES',2,true,$3::DECIMAL)", string(f.BookID), string(f.BookID), cap.String()); err != nil {
			return application.Receipt{}, err
		}
		book := domain.Book{ID: f.BookID, Currency: "KES", Scale: 2}
		accounts := []domain.Account{
			{ID: f.WalletA, Book: book, Owner: f.OwnerA, Purpose: domain.Wallet},
			{ID: f.WalletB, Book: book, Owner: f.OwnerB, Purpose: domain.Wallet},
			{ID: f.Pool, Book: book, Owner: f.SystemOwner, Purpose: domain.ProviderPool},
			{ID: f.FeeAccount, Book: book, Owner: f.SystemOwner, Purpose: domain.FeeIncome},
		}
		canonical, err := application.Canonical(map[string]any{"schema_version": "1", "kind": "FIXTURE_FUND", "book_id": f.BookID, "units": "100000"})
		if err != nil {
			return application.Receipt{}, err
		}
		p := application.Plan{Canonical: canonical, Receipt: application.Receipt{
			SchemaVersion: "1", BookID: f.BookID, OperationID: f.OperationID, Kind: "FIXTURE_FUND",
			RequestHash: bodyDigest(canonical), Outcome: "APPLIED", JournalID: journalID, EventID: eventID,
			SubjectIDs: []domain.ID{f.OwnerA, f.SystemOwner}, RecordedAt: time.Now().UTC(),
			ControlVersions: map[string]string{}, AccountVersions: map[domain.ID]string{},
		}}
		controls := map[application.ControlKey]bool{}
		for _, a := range accounts {
			if err = t.write(ctx, "INSERT INTO accounts(book_id,account_id,owner_id,purpose) VALUES($1,$2,$3,$4)", string(f.BookID), string(a.ID), string(a.Owner), string(a.Purpose)); err != nil {
				return application.Receipt{}, err
			}
			b := application.Balance{AccountID: a.ID, Version: 1}
			if a.ID == f.WalletA {
				b.Totals.Credits = domain.WideAmount(funding)
			}
			if a.ID == f.Pool {
				b.Totals.Debits = domain.WideAmount(funding)
			}
			p.Balances = append(p.Balances, application.Change[application.Balance]{After: b})
			p.Receipt.AccountVersions[a.ID] = "1"
			for _, key := range []application.ControlKey{{Kind: "SUBJECT", ID: a.Owner}, {Kind: "ACCOUNT", ID: a.ID}} {
				if controls[key] {
					continue
				}
				controls[key] = true
				c := application.Control{Key: key, Version: 1, Epoch: 1}
				if key.Kind == "SUBJECT" {
					c.DailyCap = cap
				}
				p.Controls = append(p.Controls, application.Change[application.Control]{After: c})
				p.Receipt.ControlVersions[key.String()] = "1"
			}
		}
		for _, policy := range []struct {
			id  domain.ID
			fee int64
		}{{f.Policy100, 100}, {f.Policy200, 200}, {f.Policy1000, 1000}, {f.PolicyZero, 0}} {
			if err = t.write(ctx, "INSERT INTO posting_policies(book_id,policy_id,fee,fee_account_id,pool_id,provider_account_id,capability_id,max_hold_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,900)", string(f.BookID), string(policy.id), policy.fee, string(f.FeeAccount), string(f.Pool), string(f.ProviderAccount), string(f.Capability)); err != nil {
				return application.Receipt{}, err
			}
		}
		j, err := domain.NewJournal(book, []domain.Line{{Account: accounts[2], Side: domain.Debit, Amount: funding}, {Account: accounts[0], Side: domain.Credit, Amount: funding}})
		if err != nil {
			return application.Receipt{}, err
		}
		p.Journal = &j
		if err = t.Save(ctx, p); err != nil {
			return application.Receipt{}, err
		}
		return p.Receipt, nil
	})
	return f, err
}
