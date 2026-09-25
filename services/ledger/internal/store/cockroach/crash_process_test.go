package cockroach

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

// These exported test types bridge the internal SQL observer and the external
// integration tests. None of this file is linked into the Ledger executable.
type CrashInstruction struct {
	Command application.Command
	Caller  application.Caller
	Trust   application.Trust
	Now     time.Time
}

type CrashCheckpoint struct {
	Phase   string
	SQL     string
	Receipt *application.Receipt
}

type crashGate struct {
	in  *json.Decoder
	out *json.Encoder
}

func (g *crashGate) stop(point CrashCheckpoint) error {
	if err := g.out.Encode(point); err != nil {
		return err
	}
	// The parent either explicitly advances this checkpoint or forcibly kills
	// this process. No rollback, cancellation or synthetic SQL error is injected.
	var advance bool
	if err := g.in.Decode(&advance); err != nil {
		return err
	}
	if !advance {
		return errors.New("crash checkpoint was not advanced")
	}
	return nil
}

type crashSQL struct {
	pgx.Tx
	gate *crashGate
}

func (c *crashSQL) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	tag, err := c.Tx.Exec(ctx, query, args...)
	if err == nil {
		// Observe only successfully executed, real SQL. Do not print bound data.
		err = c.gate.stop(CrashCheckpoint{Phase: "after_write", SQL: query})
	}
	return tag, err
}

type crashTransaction struct {
	application.Transaction
	gate *crashGate
}

func (c *crashTransaction) Save(ctx context.Context, plan application.Plan) error {
	if err := c.gate.stop(CrashCheckpoint{Phase: "before_save"}); err != nil {
		return err
	}
	raw := c.Transaction.(*tx)
	original := raw.Tx
	raw.Tx = &crashSQL{Tx: original, gate: c.gate}
	defer func() { raw.Tx = original }()
	return raw.Save(ctx, plan)
}

type crashStore struct {
	*Store
	gate *crashGate
}

func (s *crashStore) Transact(ctx context.Context, fn func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	receipt, err := s.Store.Transact(ctx, func(transaction application.Transaction) (application.Receipt, error) {
		r, err := fn(&crashTransaction{Transaction: transaction, gate: s.gate})
		if err == nil {
			err = s.gate.stop(CrashCheckpoint{Phase: "before_commit", Receipt: &r})
		}
		return r, err
	})
	if err == nil {
		err = s.gate.stop(CrashCheckpoint{Phase: "after_commit", Receipt: &receipt})
	}
	return receipt, err
}

// TestLedgerWriteCrashChild executes the real application and persistence in a
// separate process using only runtime DB credentials and public verification
// keys. Existing app tests separately exercise the authenticated composition
// root and a lost RPC response. This helper deliberately bypasses that transport
// so it can gate every SQL write without adding deployable fault switches.
func TestLedgerWriteCrashChild(t *testing.T) {
	if os.Getenv("PESAR_LEDGER_WRITE_CRASH_CHILD") != "1" {
		t.Skip("child-process entry point")
	}
	gate := &crashGate{in: json.NewDecoder(os.Stdin), out: json.NewEncoder(os.Stdout)}
	var instruction CrashInstruction
	if err := gate.in.Decode(&instruction); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, os.Getenv("PESAR_LEDGER_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.ReadyForBooks(ctx, []domain.ID{instruction.Command.BookID}); err != nil {
		t.Fatal(err)
	}
	service := application.New(&crashStore{Store: store, gate: gate}, instruction.Trust, func() time.Time { return instruction.Now })
	receipt, err := service.Execute(ctx, instruction.Caller, instruction.Command)
	if err != nil {
		t.Fatal(err)
	}
	// This is the response to the caller, distinct from checkpoint diagnostics.
	// Every crash test must kill before this response is emitted.
	if err = gate.out.Encode(CrashCheckpoint{Phase: "response", Receipt: &receipt}); err != nil {
		t.Fatal(err)
	}
}

// CrashDeliverySnapshot supplements the financial export with mutable delivery
// state. The tests run without publishers and compare these rows after rollback
// and replay, including receipt -> fact -> delivery insertion gaps.
func CrashDeliverySnapshot(ctx context.Context, store *Store, book domain.ID) ([]string, error) {
	rows, err := store.pool.Query(ctx, "SELECT row_to_json(d)::STRING FROM outbox_delivery d WHERE book_id=$1 ORDER BY event_id", string(book))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
