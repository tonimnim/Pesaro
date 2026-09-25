#!/usr/bin/env bash
# Run after local-db.sh start. Add --restart for the destructive synthetic drill.
set -euo pipefail
ledger_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$ledger_root"
ledger_race=()
ledger_restart=false
for ledger_arg in "$@"; do
  case "$ledger_arg" in --restart) ledger_restart=true;; --race) ledger_race=(-race);; *) echo "usage: test.sh [--restart] [--race]" >&2; exit 2;; esac
done
ledger_certs="$ledger_root/.cache/ledger-db/certs"
for ledger_file in ca.crt client.root.crt client.root.key client.ledger_runtime.crt client.ledger_runtime.key; do
  [ -f "$ledger_certs/$ledger_file" ] || { echo "Run scripts/ledger/local-db.sh start first." >&2; exit 1; }
done
export PESARO_LEDGER_TEST_ADMIN_URL="postgresql://root@localhost:26277/pesaro_ledger?sslmode=verify-full&sslrootcert=$ledger_certs/ca.crt&sslcert=$ledger_certs/client.root.crt&sslkey=$ledger_certs/client.root.key"
export PESARO_LEDGER_TEST_RUNTIME_URL="postgresql://ledger_runtime@localhost:26277/pesaro_ledger?sslmode=verify-full&sslrootcert=$ledger_certs/ca.crt&sslcert=$ledger_certs/client.ledger_runtime.crt&sslkey=$ledger_certs/client.ledger_runtime.key"
export PESAR_LEDGER_EVIDENCE_DIR="$ledger_root/.cache/ledger-evidence"
mkdir -p "$PESAR_LEDGER_EVIDENCE_DIR"
# DDL has one administrator; parallel financial test packages only read schema readiness.
PESAR_LEDGER_ADMIN_URL="$PESARO_LEDGER_TEST_ADMIN_URL" go run ./services/ledger/cmd/ledger-admin -synthetic -action=migrate | tee "$PESAR_LEDGER_EVIDENCE_DIR/migrate.log"
go test -count=1 -v "${ledger_race[@]}" ./services/ledger/... ./services/reconciliation/internal/verify | tee "$PESAR_LEDGER_EVIDENCE_DIR/tests.log"
if "$ledger_restart"; then
  ledger_artifact="$PESAR_LEDGER_EVIDENCE_DIR/restart-instruction.json"
  PESAR_LEDGER_RESTART_PREPARE="$ledger_artifact" go test -count=1 -run '^TestDatabaseRestartPrepare$' -v ./services/ledger/internal/app | tee "$PESAR_LEDGER_EVIDENCE_DIR/restart-prepare.log"
  bash scripts/ledger/local-db.sh crash
  bash scripts/ledger/local-db.sh start
  PESAR_LEDGER_RESTART_RECOVER="$ledger_artifact" go test -count=1 -run '^TestDatabaseRestartRecover$' -v ./services/ledger/internal/app | tee "$PESAR_LEDGER_EVIDENCE_DIR/restart-recover.log"
fi
{
  go version
  git rev-parse HEAD
  echo "CockroachDB v26.2.3; synthetic single node; TLS"
  echo "database_restart=$ledger_restart"
  echo "race_detection=${ledger_race[*]:-disabled}"
  echo "Selected evidence passed; full M1 and production gates remain separate."
} > "$PESAR_LEDGER_EVIDENCE_DIR/run.txt"
