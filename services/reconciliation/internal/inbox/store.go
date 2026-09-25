// Package inbox owns Reconciliation's durable event ingestion and receipt
// projection. It never queries or writes another service's database.
package inbox

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	ledgerevent "github.com/tonimnim/Pesaro/contracts/events/ledger/v1"
)

var ErrUnavailable = errors.New("reconciliation inbox unavailable; retry original event")
var ErrConflict = errors.New("event identity conflicts with durable evidence")

//go:embed migrations/001_initial.sql
var migration string

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string, admin bool) (*Store, error) {
	u, err := url.Parse(dsn)
	role := "reconciliation_runtime"
	if admin {
		role = "root"
	}
	if err != nil || u.Scheme != "postgresql" || u.User == nil || u.User.Username() != role || u.Path != "/pesaro_reconciliation" || u.Fragment != "" || u.Query().Get("sslmode") != "verify-full" {
		return nil, ErrUnavailable
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, ErrUnavailable
	}
	for key, values := range u.Query() {
		if len(values) != 1 || (key != "sslmode" && key != "sslrootcert" && key != "sslcert" && key != "sslkey") {
			return nil, ErrUnavailable
		}
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, ErrUnavailable
	}
	config.MaxConns = 8
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	config.ConnConfig.RuntimeParams["application_name"] = "pesar-reconciliation-inbox"
	config.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, ErrUnavailable
	}
	var version, identity string
	if pool.QueryRow(ctx, "SELECT version(),current_user").Scan(&version, &identity) != nil || identity != role || !strings.Contains(version, "CockroachDB") || !slices.Contains(strings.Fields(version), "v26.2.3") {
		pool.Close()
		return nil, ErrUnavailable
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()         { s.pool.Close() }
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func checksum() string          { return digest([]byte(strings.ReplaceAll(migration, "\r\n", "\n"))) }

func (s *Store) Ready(ctx context.Context) error {
	var version int
	var sum string
	if s.pool.QueryRow(ctx, "SELECT version,checksum FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version, &sum) != nil || version != 1 || sum != checksum() {
		return ErrUnavailable
	}
	return nil
}

// Migrate is called only by the separate synthetic administrator executable.
func (s *Store) Migrate(ctx context.Context) error {
	for _, statement := range strings.Split(migration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := s.pool.Exec(ctx, "INSERT INTO schema_migrations(version,checksum) VALUES(1,$1) ON CONFLICT(version) DO NOTHING", checksum()); err != nil {
		return err
	}
	for _, statement := range []string{
		"REVOKE CREATE ON SCHEMA public FROM public",
		"REVOKE CREATE ON SCHEMA public FROM reconciliation_runtime",
		"REVOKE CREATE ON DATABASE pesaro_reconciliation FROM reconciliation_runtime",
		"GRANT CONNECT ON DATABASE pesaro_reconciliation TO reconciliation_runtime",
		"GRANT USAGE ON SCHEMA public TO reconciliation_runtime",
		"GRANT SELECT ON TABLE schema_migrations,event_inbox,ledger_operations TO reconciliation_runtime",
		"GRANT INSERT ON TABLE event_inbox,ledger_operations TO reconciliation_runtime",
	} {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return s.Ready(ctx)
}

func code(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

// Accept acknowledges only after a durable transaction or an equivalent durable
// inbox record. A lost COMMIT reply is a retryable technical failure, never a
// fresh event identity. Inbox and projection are both append-only.
func (s *Store) Accept(ctx context.Context, data []byte) error {
	event, canonical, err := ledgerevent.Decode(data)
	if err != nil {
		return err
	}
	hash := digest(canonical)
	for attempt := 0; attempt < 8; attempt++ {
		err = s.acceptOnce(ctx, event, hash)
		if err == nil || errors.Is(err, ErrConflict) {
			return err
		}
		if code(err) != "40001" && code(err) != "23505" {
			return ErrUnavailable
		}
		timer := time.NewTimer(time.Duration(5*(attempt+1)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ErrUnavailable
		case <-timer.C:
		}
	}
	return ErrUnavailable
}

func (s *Store) acceptOnce(ctx context.Context, event ledgerevent.Event, hash string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() {
		rollback, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollback)
	}()
	if err = persist(ctx, tx, event, hash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// The narrow pgx.Tx parameter also lets tests pause after real SQL writes in a
// child process. Production always passes its own SERIALIZABLE transaction.
func persist(ctx context.Context, tx pgx.Tx, event ledgerevent.Event, hash string) error {
	var existing string
	err := tx.QueryRow(ctx, "SELECT digest FROM event_inbox WHERE book_id=$1 AND event_id=$2", event.BookID, event.EventID).Scan(&existing)
	if err == nil {
		if existing != hash {
			return ErrConflict
		}
		var projected bool
		if tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ledger_operations WHERE book_id=$1 AND operation_id=$2 AND event_id=$3)", event.BookID, event.OperationID, event.EventID).Scan(&projected) != nil || !projected {
			return ErrUnavailable
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// Also bind operation identity: a new event ID cannot invent a second
	// observation of the same original receipt, even under a valid producer.
	err = tx.QueryRow(ctx, "SELECT event_id::STRING FROM ledger_operations WHERE book_id=$1 AND operation_id=$2", event.BookID, event.OperationID).Scan(&existing)
	if err == nil {
		return ErrConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO event_inbox(book_id,event_id,digest) VALUES($1,$2,$3)", event.BookID, event.EventID, hash); err != nil {
		return err
	}
	r, err := event.Identity()
	if err != nil {
		return ledgerevent.ErrInvalid
	}
	when, err := time.Parse(time.RFC3339Nano, r.RecordedAt)
	if err != nil {
		return ledgerevent.ErrInvalid
	}
	_, err = tx.Exec(ctx, "INSERT INTO ledger_operations(book_id,operation_id,event_id,request_hash,kind,outcome,recorded_at,receipt) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", event.BookID, event.OperationID, event.EventID, r.RequestHash, r.Kind, r.Outcome, when, []byte(event.Receipt))
	return err
}
