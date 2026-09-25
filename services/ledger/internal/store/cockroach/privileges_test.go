package cockroach_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRuntimeCannotRewriteFinancialHistory(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, os.Getenv("PESARO_LEDGER_TEST_RUNTIME_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	for _, statement := range []string{
		"UPDATE journals SET template=template WHERE book_id=$1",
		"DELETE FROM journal_lines WHERE book_id=$1",
		"UPDATE financial_operations SET receipt=receipt WHERE book_id=$1",
		"DELETE FROM outbox_facts WHERE book_id=$1",
		"UPDATE accounts SET owner_id=owner_id WHERE book_id=$1",
		"UPDATE posting_policies SET fee=fee WHERE book_id=$1",
		"DELETE FROM account_events WHERE book_id=$1",
		"DELETE FROM control_events WHERE book_id=$1",
		"DELETE FROM hold_events WHERE book_id=$1",
		"DELETE FROM limit_events WHERE book_id=$1",
		"UPDATE resolution_evidence SET digest=digest WHERE book_id=$1",
		"INSERT INTO books SELECT * FROM books WHERE book_id=$1",
	} {
		_, err = conn.Exec(ctx, statement, string(e.fixture.BookID))
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatalf("runtime history restriction failed for %s: %v", statement, err)
		}
	}
	table := "runtime_must_not_create_" + strings.ReplaceAll(string(e.fixture.BookID), "-", "")
	_, err = conn.Exec(ctx, "CREATE TABLE "+table+" (id INT)")
	if err == nil {
		_, _ = conn.Exec(ctx, "DROP TABLE "+table)
	}
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "42501" {
		t.Fatalf("runtime DDL restriction failed: %v", err)
	}
}
func TestDatabaseRejectsFractionalAndInvalidAggregates(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, os.Getenv("PESARO_LEDGER_TEST_ADMIN_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	for _, statement := range []string{
		"UPDATE account_balances SET held=0.5 WHERE book_id=$1",
		"UPDATE account_balances SET held=-1 WHERE book_id=$1",
		"UPDATE account_balances SET debits=100000000000000000000000000000000000000 WHERE book_id=$1",
		"UPDATE spending_controls SET daily_cap=0.5 WHERE book_id=$1",
	} {
		_, err = conn.Exec(ctx, statement, string(e.fixture.BookID))
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatalf("database invariant not enforced for %s: %v", statement, err)
		}
	}
}
