package cockroach

import (
	"context"
	"strconv"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

// LeasedFact is an immutable fact plus revocable delivery ownership.
type LeasedFact struct {
	BookID      domain.ID
	EventID     domain.ID
	OperationID domain.ID
	LeaseToken  domain.ID
	Type        string
	Body        []byte
}

func (s *Store) ClaimOutbox(ctx context.Context, book domain.ID, limit int, lease time.Duration) ([]LeasedFact, error) {
	if !book.Valid() || limit < 1 || limit > 100 || lease < time.Second || lease > time.Minute {
		return nil, application.ErrInvalid
	}
	token := domain.NewID()
	var facts []LeasedFact
	_, err := s.Transact(ctx, func(transaction application.Transaction) (application.Receipt, error) {
		facts = nil
		t := transaction.(*tx)
		// Revisit all undelivered identities. No timestamp high-water mark can skip
		// a transaction that committed later with an older recorded_at.
		rows, err := t.Query(ctx, "SELECT event_id::STRING FROM outbox_delivery WHERE book_id=$1 AND delivered=false AND (lease_until IS NULL OR lease_until<=now()) ORDER BY event_id LIMIT $2 FOR UPDATE", string(book), limit)
		if err != nil {
			return application.Receipt{}, err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return application.Receipt{}, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return application.Receipt{}, err
		}
		interval := strconv.FormatInt(lease.Milliseconds(), 10) + " milliseconds"
		for _, id := range ids {
			if err = t.write(ctx, "UPDATE outbox_delivery SET lease_token=$3,lease_until=now()+$4::INTERVAL,attempts=attempts+1 WHERE book_id=$1 AND event_id=$2 AND delivered=false AND (lease_until IS NULL OR lease_until<=now())", string(book), id, string(token), interval); err != nil {
				return application.Receipt{}, err
			}
			fact := LeasedFact{BookID: book, EventID: domain.ID(id), LeaseToken: token}
			var operation string
			if err = t.QueryRow(ctx, "SELECT operation_id::STRING,event_type,body FROM outbox_facts WHERE book_id=$1 AND event_id=$2", string(book), id).Scan(&operation, &fact.Type, &fact.Body); err != nil {
				return application.Receipt{}, err
			}
			fact.OperationID = domain.ID(operation)
			facts = append(facts, fact)
		}
		return application.Receipt{}, nil
	})
	if err != nil {
		return nil, err
	}
	return facts, nil
}
func (s *Store) AcknowledgeOutbox(ctx context.Context, fact LeasedFact) error {
	if !fact.BookID.Valid() || !fact.EventID.Valid() || !fact.LeaseToken.Valid() {
		return application.ErrInvalid
	}
	result, err := s.pool.Exec(ctx, "UPDATE outbox_delivery SET delivered=true WHERE book_id=$1 AND event_id=$2 AND lease_token=$3 AND lease_until>now()", string(fact.BookID), string(fact.EventID), string(fact.LeaseToken))
	if err != nil {
		return application.ErrUnavailable
	}
	if result.RowsAffected() != 1 {
		return application.ErrConflict
	}
	return nil
}

// PublishOutbox commits leases before calling the transport. Delivery errors or
// lost acknowledgements leave the same event recoverable after lease expiry.
// Consumers must atomically deduplicate that ID with their own mutation.
func (s *Store) PublishOutbox(ctx context.Context, book domain.ID, limit int, deliver func(context.Context, LeasedFact) error) (int, error) {
	if deliver == nil {
		return 0, application.ErrInvalid
	}
	facts, err := s.ClaimOutbox(ctx, book, limit, time.Minute)
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, fact := range facts {
		if err = deliver(ctx, fact); err != nil {
			return delivered, err
		}
		if err = s.AcknowledgeOutbox(ctx, fact); err != nil {
			return delivered, err
		}
		delivered++
	}
	return delivered, nil
}
