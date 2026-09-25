#!/usr/bin/env bash
# Synthetic local CockroachDB only. Run from Linux/WSL; data stays in this repo.
set -euo pipefail
umask 077
ledger_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$ledger_root"
ledger_action="${1:-start}"
ledger_version=v26.2.3
ledger_archive_sha=3eca6d7bc6fefa3ba0847e89733fc69f61226c80b8fab0af6578e1be672f27d3
ledger_bin="$ledger_root/.cache/ledger-tools/cockroach-$ledger_version.linux-amd64/cockroach"
ledger_dir="$ledger_root/.cache/ledger-db"
ledger_hash="$(printf '%s' "$ledger_root" | sha256sum | cut -c1-16)"
ledger_private="/tmp/pesar-ledger-$UID-$ledger_hash"
mkdir -p "$ledger_dir" "$ledger_root/.cache/ledger-tools"
case "$ledger_action" in start|stop|crash|status) ;; *) echo "usage: local-db.sh start|stop|crash|status" >&2; exit 2;; esac
owned_pid() {
  [ -f "$ledger_dir/cockroach.pid" ] || return 1
  ledger_pid="$(cat "$ledger_dir/cockroach.pid")"
  [[ "$ledger_pid" =~ ^[0-9]+$ ]] || return 1
  [ -r "/proc/$ledger_pid/cmdline" ] || return 1
  # Both executable and exact store must match before this script sends a signal.
  ledger_exe="$(readlink "/proc/$ledger_pid/exe")"
  [ "$ledger_exe" = "$ledger_bin" ] || return 1
  tr '\0' '\n' < "/proc/$ledger_pid/cmdline" | grep -Fqx -- "--store=$ledger_dir/data"
}
if [ "$ledger_action" = status ]; then
  if owned_pid; then echo "Synthetic CockroachDB running on localhost:26277."; else echo "No matching local CockroachDB process."; exit 1; fi
  exit 0
fi
if [ "$ledger_action" = stop ] || [ "$ledger_action" = crash ]; then
  if ! owned_pid; then echo "Refusing to signal an absent or unverified database process." >&2; exit 1; fi
  if [ "$ledger_action" = crash ]; then kill -KILL "$ledger_pid"; else kill -TERM "$ledger_pid"; fi
  for _ in $(seq 1 100); do
    if ! owned_pid; then echo "Synthetic CockroachDB stopped."; exit 0; fi
    sleep 0.1
  done
  echo "Database still stopping; no second signal sent." >&2
  exit 1
fi
ledger_running=false
if owned_pid; then ledger_running=true; fi
if [ ! -x "$ledger_bin" ]; then
  ledger_archive="$ledger_root/.cache/ledger-tools/cockroach-$ledger_version.linux-amd64.tgz"
  curl --fail --silent --show-error --location "https://binaries.cockroachdb.com/cockroach-$ledger_version.linux-amd64.tgz" -o "$ledger_archive"
  printf '%s  %s\n' "$ledger_archive_sha" "$ledger_archive" | sha256sum --check
  tar -xzf "$ledger_archive" -C "$ledger_root/.cache/ledger-tools"
fi
"$ledger_bin" version | grep -F "$ledger_version" >/dev/null
mkdir -p "$ledger_private/certs" "$ledger_dir/certs" "$ledger_dir/data" "$ledger_dir/logs"
chmod 700 "$ledger_private" "$ledger_private/certs"
if [ ! -f "$ledger_private/certs/ca.crt" ]; then
  "$ledger_bin" cert create-ca --certs-dir="$ledger_private/certs" --ca-key="$ledger_private/ca.key"
fi
if [ ! -f "$ledger_private/certs/node.crt" ]; then
  "$ledger_bin" cert create-node localhost 127.0.0.1 --certs-dir="$ledger_private/certs" --ca-key="$ledger_private/ca.key"
fi
for ledger_user in root ledger_runtime reconciliation_runtime; do
  if [ ! -f "$ledger_private/certs/client.$ledger_user.crt" ]; then
    "$ledger_bin" cert create-client "$ledger_user" --certs-dir="$ledger_private/certs" --ca-key="$ledger_private/ca.key"
  fi
  cp "$ledger_private/certs/client.$ledger_user.crt" "$ledger_dir/certs/"
  cp "$ledger_private/certs/client.$ledger_user.key" "$ledger_dir/certs/"
done
cp "$ledger_private/certs/ca.crt" "$ledger_dir/certs/"
if ! "$ledger_running"; then
  "$ledger_bin" start-single-node --certs-dir="$ledger_private/certs" --listen-addr=127.0.0.1:26277 --http-addr=127.0.0.1:8087 --store="$ledger_dir/data" --cache=128MiB --max-sql-memory=128MiB --pid-file="$ledger_dir/cockroach.pid" --log-dir="$ledger_dir/logs" --background
fi
"$ledger_bin" sql --certs-dir="$ledger_private/certs" --host=localhost:26277 --execute="CREATE DATABASE IF NOT EXISTS pesaro_ledger; CREATE USER IF NOT EXISTS ledger_runtime; CREATE DATABASE IF NOT EXISTS pesaro_reconciliation; CREATE USER IF NOT EXISTS reconciliation_runtime;"
echo "Synthetic TLS database ready on localhost:26277. Private certificates are local and ignored."
