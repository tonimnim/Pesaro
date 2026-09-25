// Package cockroach persists the entire financial boundary in SERIALIZABLE SQL.
package cockroach

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

//go:embed migrations/001_initial.sql
var migration string

const SchemaVersion = 1
const EngineVersion = "v26.2.3"

type Store struct {
	pool    *pgxpool.Pool
	retries int
}
type tx struct{ pgx.Tx }

func Open(ctx context.Context, dsn string) (*Store, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgresql" || u.Query().Get("sslmode") != "verify-full" {
		return nil, errors.New("Ledger requires a postgresql URL with sslmode=verify-full")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid Ledger database configuration")
	}
	cfg.MaxConns = 16
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "pesaro-ledger"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	// Describe parameter types so JSONB payloads and BYTES canonical requests
	// retain their distinct encodings; never infer both from a Go []byte alone.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, application.ErrUnavailable
	}
	s := &Store{pool: pool, retries: 8}
	var version string
	if err = pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		pool.Close()
		return nil, application.ErrUnavailable
	}
	if !strings.Contains(version, "CockroachDB") || !strings.Contains(version, EngineVersion) {
		pool.Close()
		return nil, fmt.Errorf("Ledger adapter requires CockroachDB %s", EngineVersion)
	}
	return s, nil
}
func (s *Store) Close() { s.pool.Close() }

// ReadyForBooks verifies the runtime identity, migration and configured scope.
func (s *Store) ReadyForBooks(ctx context.Context, books []domain.ID) error {
	if err := s.Ready(ctx); err != nil {
		return err
	}
	var identity string
	if s.pool.QueryRow(ctx, "SELECT current_user").Scan(&identity) != nil || identity != "ledger_runtime" || len(books) == 0 {
		return application.ErrUnavailable
	}
	for _, book := range books {
		var synthetic bool
		if s.pool.QueryRow(ctx, "SELECT synthetic FROM books WHERE book_id=$1", string(book)).Scan(&synthetic) != nil || !synthetic {
			return application.ErrUnavailable
		}
	}
	return nil
}
func checksum() string {
	h := sha256.Sum256([]byte(strings.ReplaceAll(migration, "\r\n", "\n")))
	return hex.EncodeToString(h[:])
}
func (s *Store) Ready(ctx context.Context) error {
	var version int
	var sum string
	err := s.pool.QueryRow(ctx, "SELECT version,checksum FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version, &sum)
	if err != nil {
		return application.ErrUnavailable
	}
	if version != SchemaVersion || sum != checksum() {
		return errors.New("Ledger schema version/checksum mismatch")
	}
	return nil
}

// Migrate is invoked only by the separate privileged administration command.
// Runtime startup never executes DDL or grants itself permissions.
func (s *Store) Migrate(ctx context.Context) error {
	for _, statement := range strings.Split(migration, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("ledger migration: %w", err)
		}
	}
	_, err := s.pool.Exec(ctx, "INSERT INTO schema_migrations(version,checksum) VALUES ($1,$2) ON CONFLICT(version) DO NOTHING", SchemaVersion, checksum())
	if err != nil {
		return err
	}
	return s.Ready(ctx)
}
func (s *Store) GrantRuntime(ctx context.Context) error {
	// Each privilege is explicit: history and immutable account/policy identity
	// cannot be updated/deleted by the serving workload.
	statements := []string{
		"GRANT CONNECT ON DATABASE pesaro_ledger TO ledger_runtime",
		"GRANT USAGE ON SCHEMA public TO ledger_runtime",
		"GRANT SELECT ON TABLE schema_migrations,books,accounts,posting_policies,account_balances,spending_controls,business_claims,holds,limit_usage,journals,journal_lines,account_events,control_events,hold_events,limit_events,resolution_evidence,financial_operations,outbox_facts,outbox_delivery TO ledger_runtime",
		"GRANT INSERT ON TABLE accounts,account_balances,spending_controls,business_claims,holds,limit_usage,journals,journal_lines,account_events,control_events,hold_events,limit_events,resolution_evidence,financial_operations,outbox_facts,outbox_delivery TO ledger_runtime",
		"GRANT UPDATE ON TABLE account_balances,spending_controls,business_claims,holds,limit_usage,outbox_delivery TO ledger_runtime",
	}
	for _, q := range statements {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
func sqlCode(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}
func retryable(err error) bool { return sqlCode(err) == "40001" || sqlCode(err) == "23505" }
func pause(ctx context.Context, attempt int) error {
	d := time.Duration(2+rand.IntN(8)+(1<<min(attempt, 6))) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (s *Store) Transact(ctx context.Context, fn func(application.Transaction) (application.Receipt, error)) (application.Receipt, error) {
	for attempt := 0; attempt < s.retries; attempt++ {
		raw, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return application.Receipt{}, application.ErrUnavailable
		}
		result, err := fn(&tx{raw})
		if err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = raw.Rollback(rollbackCtx)
			cancel()
			if retryable(err) {
				if pause(ctx, attempt) != nil {
					return application.Receipt{}, application.ErrUnavailable
				}
				continue
			}
			if sqlCode(err) == "40003" {
				return application.Receipt{}, application.ErrUnknown
			}
			if sqlCode(err) != "" {
				var pg *pgconn.PgError
				errors.As(err, &pg)
				return application.Receipt{}, fmt.Errorf("%w (SQLSTATE %s; constraint %s)", application.ErrIntegrity, pg.Code, pg.ConstraintName)
			}
			return application.Receipt{}, err
		}
		if err = raw.Commit(ctx); err == nil {
			return result, nil
		}
		if sqlCode(err) == "40001" {
			if pause(ctx, attempt) != nil {
				return application.Receipt{}, application.ErrUnavailable
			}
			continue
		}
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return application.Receipt{}, application.ErrUnavailable
		}
		// A transport cancellation/EOF around COMMIT is ambiguous even without
		// a PgError. Never turn it into a durable business decline.
		return application.Receipt{}, application.ErrUnknown
	}
	return application.Receipt{}, application.ErrUnavailable
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func operation(ctx context.Context, q queryer, book, id domain.ID) (*application.Operation, error) {
	var canonical, receipt []byte
	err := q.QueryRow(ctx, "SELECT canonical,receipt FROM financial_operations WHERE book_id=$1 AND operation_id=$2", string(book), string(id)).Scan(&canonical, &receipt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result application.Receipt
	if json.Unmarshal(receipt, &result) != nil {
		return nil, application.ErrIntegrity
	}
	return &application.Operation{Canonical: canonical, Receipt: result}, nil
}
func (t *tx) Operation(ctx context.Context, book, id domain.ID) (*application.Operation, error) {
	return operation(ctx, t, book, id)
}
func (s *Store) Operation(ctx context.Context, book, id domain.ID) (*application.Operation, error) {
	r, err := operation(ctx, s.pool, book, id)
	if err != nil {
		return nil, application.ErrUnavailable
	}
	return r, nil
}
