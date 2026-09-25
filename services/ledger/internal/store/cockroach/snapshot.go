package cockroach

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tonimnim/Pesaro/services/ledger/internal/application"
	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func bodyDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Export columns are fixed, not caller-controlled SQL identifiers. Every cell is
// textual (including exact decimal values). JSON body cells contain JSON text.
var exportColumns = map[string][]string{
	"books":                {"book_id", "entity_id", "country", "currency", "scale", "synthetic", "default_cap"},
	"accounts":             {"book_id", "account_id", "owner_id", "purpose"},
	"posting_policies":     {"book_id", "policy_id", "fee", "fee_account_id", "pool_id", "provider_account_id", "capability_id", "max_hold_seconds"},
	"account_balances":     {"book_id", "account_id", "purpose", "debits", "credits", "held", "version"},
	"spending_controls":    {"book_id", "subject_kind", "subject_id", "version", "epoch", "debit_frozen", "credit_frozen", "daily_cap"},
	"business_claims":      {"book_id", "payment_id", "stage", "admission_operation_id", "admission_outcome", "material_hash", "state", "hold_id", "result_operation_id"},
	"holds":                {"book_id", "hold_id", "payment_id", "attempt_id", "terms", "expires_at", "state", "version"},
	"limit_usage":          {"book_id", "owner_id", "bucket", "reserved", "consumed", "version"},
	"journals":             {"book_id", "journal_id", "operation_id", "template", "recorded_at", "line_count", "digest"},
	"journal_lines":        {"book_id", "journal_id", "ordinal", "account_id", "side", "units"},
	"account_events":       {"book_id", "account_id", "version", "operation_id", "body"},
	"control_events":       {"book_id", "subject_kind", "subject_id", "version", "operation_id", "body"},
	"hold_events":          {"book_id", "hold_id", "version", "operation_id", "body"},
	"limit_events":         {"book_id", "owner_id", "bucket", "version", "operation_id", "body"},
	"resolution_evidence":  {"book_id", "evidence_id", "digest", "body"},
	"financial_operations": {"book_id", "operation_id", "canonical", "request_hash", "outcome", "receipt"},
	"outbox_facts":         {"book_id", "event_id", "operation_id", "event_type", "body"},
}

func (s *Store) Snapshot(ctx context.Context, book domain.ID) (application.Snapshot, error) {
	raw, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.Snapshot{}, application.ErrUnavailable
	}
	defer raw.Rollback(context.Background())
	if _, err = readBook(ctx, raw, book); err != nil {
		return application.Snapshot{}, err
	}
	result := application.Snapshot{SchemaVersion: "1", BookID: book, Tables: map[string][]map[string]any{}, Digests: map[string]string{}, Counts: map[string]int{}}
	if err = raw.QueryRow(ctx, "SELECT cluster_logical_timestamp()::STRING").Scan(&result.Cut); err != nil {
		return result, application.ErrUnavailable
	}
	total := 0
	bytes := 0
	for table, columns := range exportColumns {
		projections := make([]string, len(columns))
		for i, c := range columns {
			projections[i] = c + "::STRING"
			if table == "financial_operations" && c == "canonical" {
				projections[i] = "encode(canonical,'hex')"
			}
		}
		// Sorting by every projected column produces stable digest order at the cut.
		query := "SELECT " + strings.Join(projections, ",") + " FROM " + table + " WHERE book_id=$1 ORDER BY " + strings.Join(columns, ",") + " LIMIT 100001"
		rows, err := raw.Query(ctx, query, string(book))
		if err != nil {
			return application.Snapshot{}, application.ErrUnavailable
		}
		data := []map[string]any{}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return application.Snapshot{}, application.ErrUnavailable
			}
			row := map[string]any{}
			for i, c := range columns {
				row[c] = values[i]
			}
			encoded, marshalErr := json.Marshal(row)
			if marshalErr != nil {
				rows.Close()
				return application.Snapshot{}, application.ErrIntegrity
			}
			bytes += len(encoded)
			// Reserve space for table framing, digests and the manifest.
			if bytes > 15<<20 {
				rows.Close()
				return application.Snapshot{}, errorsExportTooLarge
			}
			data = append(data, row)
			total++
			if total > 100000 {
				rows.Close()
				return application.Snapshot{}, errorsExportTooLarge
			}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return application.Snapshot{}, application.ErrUnavailable
		}
		rows.Close()
		result.Tables[table] = data
		result.Counts[table] = len(data)
		canonical, err := application.Canonical(data)
		if err != nil {
			return application.Snapshot{}, err
		}
		result.Digests[table] = bodyDigest(canonical)
	}
	if err = raw.Commit(ctx); err != nil {
		return application.Snapshot{}, application.ErrUnavailable
	}
	return result, nil
}

var errorsExportTooLarge = application.ErrInvalid
