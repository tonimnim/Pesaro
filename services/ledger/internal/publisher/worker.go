// Package publisher drains committed Ledger facts independently of money writes.
package publisher

import (
	"context"
	"time"

	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
	"github.com/tonimnim/Pesaro/services/ledger/internal/store/cockroach"
)

type Store interface {
	ClaimOutbox(context.Context, domain.ID, int, time.Duration) ([]cockroach.LeasedFact, error)
	AcknowledgeOutbox(context.Context, cockroach.LeasedFact) error
}

type Options struct {
	PollMS    int `json:"poll_ms"`
	LeaseMS   int `json:"lease_ms"`
	TimeoutMS int `json:"timeout_ms"`
}

func (o Options) Defaults() (Options, error) {
	if o.PollMS == 0 {
		o.PollMS = 250
	}
	if o.LeaseMS == 0 {
		o.LeaseMS = 30000
	}
	if o.TimeoutMS == 0 {
		o.TimeoutMS = 3000
	}
	if o.PollMS < 50 || o.PollMS > 5000 || o.TimeoutMS < 100 || o.TimeoutMS > 5000 || o.LeaseMS < 1000 || o.LeaseMS > 60000 || o.LeaseMS < 2*o.TimeoutMS {
		return o, application.ErrInvalid
	}
	return o, nil
}

// Run claims one event at a time so queued network calls cannot exhaust leases.
// A bounded burst per book preserves fairness. No delivery occurs in a retryable
// database transaction. Any error leaves the immutable fact recoverable.
func Run(ctx context.Context, store Store, books []domain.ID, options Options, send func(context.Context, []byte) error, failed func(error)) error {
	options, err := options.Defaults()
	if err != nil || len(books) == 0 || send == nil {
		return application.ErrInvalid
	}
	for {
		for _, book := range books {
			for range 16 {
				if ctx.Err() != nil {
					return nil
				}
				query, cancel := context.WithTimeout(ctx, 5*time.Second)
				facts, err := store.ClaimOutbox(query, book, 1, time.Duration(options.LeaseMS)*time.Millisecond)
				cancel()
				if err == nil && len(facts) == 0 {
					break
				}
				if err == nil {
					fact := facts[0]
					event, encodeErr := ledgerevent.New(string(fact.BookID), string(fact.EventID), string(fact.OperationID), fact.Body)
					err = encodeErr
					if fact.Type != "LedgerOperationResolved" {
						err = ledgerevent.ErrInvalid
					}
					var data []byte
					if err == nil {
						data, err = event.Encode()
					}
					if err == nil {
						delivery, cancel := context.WithTimeout(ctx, time.Duration(options.TimeoutMS)*time.Millisecond)
						err = send(delivery, data)
						cancel()
					}
					if err == nil {
						ack, cancel := context.WithTimeout(ctx, 3*time.Second)
						err = store.AcknowledgeOutbox(ack, fact)
						cancel()
					}
				}
				if err != nil {
					if failed != nil && ctx.Err() == nil {
						failed(err)
					}
					// Try other books even if this book's consumer is down. The
					// next iteration may claim other events while this lease ages.
					break
				}
			}
		}
		timer := time.NewTimer(time.Duration(options.PollMS) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
