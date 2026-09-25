package cockroach

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func nullableID(id domain.ID) any {
	if id == "" {
		return nil
	}
	return string(id)
}
func (t *tx) write(ctx context.Context, q string, args ...any) error {
	tag, err := t.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return &pgconn.PgError{Code: "40001", Message: "ledger version changed"}
	}
	return nil
}
func encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, application.ErrIntegrity
	}
	return b, nil
}
func (t *tx) Save(ctx context.Context, p application.Plan) error {
	book, op := string(p.Receipt.BookID), string(p.Receipt.OperationID)
	if a := p.NewAccount; a != nil {
		if err := t.write(ctx, "INSERT INTO accounts(book_id,account_id,owner_id,purpose) VALUES($1,$2,$3,$4)", book, string(a.ID), string(a.Owner), string(a.Purpose)); err != nil {
			return err
		}
	}
	for _, change := range p.Controls {
		a := change.After
		if change.Before == nil {
			if err := t.write(ctx, "INSERT INTO spending_controls(book_id,subject_kind,subject_id,version,epoch,debit_frozen,credit_frozen,daily_cap) VALUES($1,$2,$3,$4,$5,$6,$7,$8::DECIMAL)", book, a.Key.Kind, string(a.Key.ID), a.Version, a.Epoch, a.DebitFrozen, a.CreditFrozen, a.DailyCap.String()); err != nil {
				return err
			}
		} else {
			if err := t.write(ctx, "UPDATE spending_controls SET version=$4,epoch=$5,debit_frozen=$6,credit_frozen=$7,daily_cap=$8::DECIMAL WHERE book_id=$1 AND subject_kind=$2 AND subject_id=$3 AND version=$9", book, a.Key.Kind, string(a.Key.ID), a.Version, a.Epoch, a.DebitFrozen, a.CreditFrozen, a.DailyCap.String(), change.Before.Version); err != nil {
				return err
			}
		}
		body, err := encode(change)
		if err != nil {
			return err
		}
		if err = t.write(ctx, "INSERT INTO control_events(book_id,subject_kind,subject_id,version,operation_id,body) VALUES($1,$2,$3,$4,$5,$6)", book, a.Key.Kind, string(a.Key.ID), a.Version, op, body); err != nil {
			return err
		}
	}
	if j := p.Journal; j != nil {
		lines := j.Lines()
		if err := t.write(ctx, "INSERT INTO journals(book_id,journal_id,operation_id,template,recorded_at,line_count,digest) VALUES($1,$2,$3,$4,$5,$6,$7)", book, string(p.Receipt.JournalID), op, string(p.Receipt.Kind)+"/v1", p.Receipt.RecordedAt, len(lines), j.Digest()); err != nil {
			return err
		}
		for ordinal, line := range lines {
			if err := t.write(ctx, "INSERT INTO journal_lines(book_id,journal_id,ordinal,account_id,side,units) VALUES($1,$2,$3,$4,$5,$6)", book, string(p.Receipt.JournalID), ordinal, string(line.Account.ID), string(line.Side), line.Amount.Int64()); err != nil {
				return err
			}
		}
	}
	for _, change := range p.Balances {
		a := change.After
		if change.Before == nil {
			if err := t.write(ctx, "INSERT INTO account_balances(book_id,account_id,purpose,debits,credits,held,version) SELECT book_id,account_id,purpose,$3::DECIMAL,$4::DECIMAL,$5::DECIMAL,$6 FROM accounts WHERE book_id=$1 AND account_id=$2", book, string(a.AccountID), a.Totals.Debits.String(), a.Totals.Credits.String(), a.Totals.Held.String(), a.Version); err != nil {
				return err
			}
		} else {
			if err := t.write(ctx, "UPDATE account_balances SET debits=$3::DECIMAL,credits=$4::DECIMAL,held=$5::DECIMAL,version=$6 WHERE book_id=$1 AND account_id=$2 AND version=$7", book, string(a.AccountID), a.Totals.Debits.String(), a.Totals.Credits.String(), a.Totals.Held.String(), a.Version, change.Before.Version); err != nil {
				return err
			}
		}
		body, err := encode(change)
		if err != nil {
			return err
		}
		if err = t.write(ctx, "INSERT INTO account_events(book_id,account_id,version,operation_id,body) VALUES($1,$2,$3,$4,$5)", book, string(a.AccountID), a.Version, op, body); err != nil {
			return err
		}
	}
	if change := p.Hold; change != nil {
		h := change.After
		if change.Before == nil {
			body, err := encode(h)
			if err != nil {
				return err
			}
			var terms map[string]any
			if json.Unmarshal(body, &terms) != nil {
				return application.ErrIntegrity
			}
			delete(terms, "state")
			body, err = encode(terms)
			if err != nil {
				return err
			}
			if err = t.write(ctx, "INSERT INTO holds(book_id,hold_id,payment_id,attempt_id,terms,expires_at,state,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", book, string(h.ID), string(h.Terms.PaymentID), string(h.AttemptID), body, h.State.ExpiresAt, string(h.State.State), h.State.Version); err != nil {
				return err
			}
		} else {
			if err := t.write(ctx, "UPDATE holds SET state=$3,version=$4 WHERE book_id=$1 AND hold_id=$2 AND version=$5 AND state=$6", book, string(h.ID), string(h.State.State), h.State.Version, change.Before.State.Version, string(change.Before.State.State)); err != nil {
				return err
			}
		}
		body, err := encode(change)
		if err != nil {
			return err
		}
		if err = t.write(ctx, "INSERT INTO hold_events(book_id,hold_id,version,operation_id,body) VALUES($1,$2,$3,$4,$5)", book, string(h.ID), h.State.Version, op, body); err != nil {
			return err
		}
	}
	if change := p.Usage; change != nil {
		u := change.After
		if change.Before == nil {
			if err := t.write(ctx, "INSERT INTO limit_usage(book_id,owner_id,bucket,reserved,consumed,version) VALUES($1,$2,$3::DATE,$4::DECIMAL,$5::DECIMAL,$6)", book, string(u.OwnerID), u.Bucket, u.Values.Reserved.String(), u.Values.Consumed.String(), u.Version); err != nil {
				return err
			}
		} else {
			if err := t.write(ctx, "UPDATE limit_usage SET reserved=$4::DECIMAL,consumed=$5::DECIMAL,version=$6 WHERE book_id=$1 AND owner_id=$2 AND bucket=$3::DATE AND version=$7", book, string(u.OwnerID), u.Bucket, u.Values.Reserved.String(), u.Values.Consumed.String(), u.Version, change.Before.Version); err != nil {
				return err
			}
		}
		body, err := encode(change)
		if err != nil {
			return err
		}
		if err = t.write(ctx, "INSERT INTO limit_events(book_id,owner_id,bucket,version,operation_id,body) VALUES($1,$2,$3::DATE,$4,$5,$6)", book, string(u.OwnerID), u.Bucket, u.Version, op, body); err != nil {
			return err
		}
	}
	if change := p.Claim; change != nil {
		c := change.After
		if change.Before == nil {
			if err := t.write(ctx, "INSERT INTO business_claims(book_id,payment_id,stage,admission_operation_id,admission_outcome,material_hash,state,hold_id,result_operation_id) VALUES($1,$2,'EXECUTION',$3,$4,$5,$6,$7,$8)", book, string(c.PaymentID), string(c.AdmissionOperationID), c.AdmissionOutcome, c.MaterialHash, c.State, nullableID(c.HoldID), string(c.ResultOperationID)); err != nil {
				return err
			}
		} else {
			if err := t.write(ctx, "UPDATE business_claims SET state=$3,result_operation_id=$4 WHERE book_id=$1 AND payment_id=$2 AND stage='EXECUTION' AND state=$5 AND admission_operation_id=$6", book, string(c.PaymentID), c.State, string(c.ResultOperationID), change.Before.State, string(change.Before.AdmissionOperationID)); err != nil {
				return err
			}
		}
	}
	if e := p.Evidence; e != nil {
		body, err := application.Canonical(e)
		if err != nil {
			return err
		}
		digest := bodyDigest(body)
		if _, err = t.Exec(ctx, "INSERT INTO resolution_evidence(book_id,evidence_id,digest,body) VALUES($1,$2,$3,$4) ON CONFLICT(book_id,evidence_id) DO NOTHING", book, string(e.ID), digest, body); err != nil {
			return err
		}
		var old string
		if err = t.QueryRow(ctx, "SELECT digest FROM resolution_evidence WHERE book_id=$1 AND evidence_id=$2", book, string(e.ID)).Scan(&old); err != nil {
			return err
		}
		if old != digest {
			return application.ErrConflict
		}
	}
	receipt, err := encode(p.Receipt)
	if err != nil {
		return err
	}
	// The unique receipt is inserted after planned effects. Any collision must
	// abort this entire transaction; Transact never swallows a failed statement.
	if err = t.write(ctx, "INSERT INTO financial_operations(book_id,operation_id,canonical,request_hash,outcome,receipt) VALUES($1,$2,$3,$4,$5,$6)", book, op, p.Canonical, p.Receipt.RequestHash, p.Receipt.Outcome, receipt); err != nil {
		return err
	}
	if err = t.write(ctx, "INSERT INTO outbox_facts(book_id,event_id,operation_id,event_type,body) VALUES($1,$2,$3,'LedgerOperationResolved',$4)", book, string(p.Receipt.EventID), op, receipt); err != nil {
		return err
	}
	return t.write(ctx, "INSERT INTO outbox_delivery(book_id,event_id) VALUES($1,$2)", book, string(p.Receipt.EventID))
}
