#!/usr/bin/env bash
# Shared helpers for chaos scenarios. Source from scripts in this directory.
# shellcheck shell=bash

set -euo pipefail

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.load.yml"
BASE_URL="${BASE_URL:-http://localhost:8080/v1}"
GATEWAY_READYZ="${GATEWAY_READYZ:-http://localhost:8081/readyz}"
ACCOUNTS_READYZ="${ACCOUNTS_READYZ:-http://localhost:8091/readyz}"
TRANSFERS_READYZ="${TRANSFERS_READYZ:-http://localhost:8092/readyz}"

CHAOS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${CHAOS_ROOT}/../.." && pwd)"

pass() {
  echo "PASS: $*"
  exit 0
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# wait_readyz URL [timeout_seconds]
wait_readyz() {
  local url="$1"
  local timeout="${2:-120}"
  local i=0
  echo "waiting for ${url} (up to ${timeout}s)..."
  while (( i < timeout )); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "ready: ${url}"
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  return 1
}

# wait_not_readyz URL [timeout_seconds] — wait until readyz returns non-2xx
wait_not_readyz() {
  local url="$1"
  local timeout="${2:-60}"
  local i=0
  echo "waiting for ${url} to become unavailable..."
  while (( i < timeout )); do
    if ! curl -sf "$url" >/dev/null 2>&1; then
      echo "not ready: ${url}"
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  return 1
}

ensure_core() {
  cd "$REPO_ROOT"
  $COMPOSE up -d postgres redis redpanda accounts transfers notifications gateway
  wait_readyz "$GATEWAY_READYZ" 180 || fail "gateway readyz not ready"
}

need_cmds() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || fail "missing dependency: $c"
  done
}

new_key() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  elif command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    fail "need uuidgen, /proc/sys/kernel/random/uuid, or openssl"
  fi
}

# Count unpublished outbox rows in a DB (accounts|transfers).
outbox_pending() {
  local db="${1:-accounts}"
  $COMPOSE exec -T postgres psql -U ledger -d "$db" -tAc \
    "SELECT count(*)::int FROM outbox WHERE published_at IS NULL" | tr -d '[:space:]'
}

# Best-effort Prometheus gauge; falls back to empty string if metric missing.
outbox_pending_metric() {
  local port="${1:-8091}"
  curl -sf "http://localhost:${port}/metrics" 2>/dev/null \
    | awk '/^outbox_pending_events/{print $2; exit}' || true
}

dev_token() {
  curl -sf -X POST "${BASE_URL}/dev/token" \
    -H 'Content-Type: application/json' -d '{}' | jq -r .token
}

create_account() {
  local token="$1"
  local currency="${2:-RUB}"
  curl -sf -X POST "${BASE_URL}/accounts" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $(new_key)" \
    -d "{\"currency\":\"${currency}\"}" | jq -r .id
}

deposit() {
  local token="$1" account="$2" amount="${3:-100000000}" currency="${4:-RUB}"
  curl -sf -X POST "${BASE_URL}/accounts/${account}/deposits" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $(new_key)" \
    -d "{\"amount\":\"${amount}\",\"currency\":\"${currency}\"}" >/dev/null
}

create_transfer() {
  local token="$1" source="$2" dest="$3" amount="${4:-100}" currency="${5:-RUB}"
  curl -sf -X POST "${BASE_URL}/transfers" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $(new_key)" \
    -d "{\"source_account_id\":\"${source}\",\"dest_account_id\":\"${dest}\",\"amount\":\"${amount}\",\"currency\":\"${currency}\"}"
}

get_transfer_status() {
  local token="$1" id="$2"
  curl -sf "${BASE_URL}/transfers/${id}" \
    -H "Authorization: Bearer ${token}" | jq -r .status
}

# Poll transfer IDs until all are completed|failed. Args: token id [id...]
wait_transfers_terminal() {
  local token="$1"
  shift
  local ids=("$@")
  local timeout="${WAIT_TRANSFERS_TIMEOUT:-180}"
  local i=0
  echo "waiting for ${#ids[@]} transfer(s) to become terminal..."
  while (( i < timeout )); do
    local all_ok=1
    local id status
    for id in "${ids[@]}"; do
      status="$(get_transfer_status "$token" "$id" 2>/dev/null || echo pending)"
      case "$status" in
        completed|failed) ;;
        *) all_ok=0; break ;;
      esac
    done
    if (( all_ok == 1 )); then
      echo "all transfers terminal"
      return 0
    fi
    sleep 2
    i=$((i + 2))
  done
  return 1
}

run_reconcile() {
  cd "$REPO_ROOT"
  $COMPOSE run --rm reconciler
}

# Start short k6 steady in background; prints PID on stdout via caller capture.
# Duration is capped by killing the process after K6_CHAOS_SECONDS (default 45).
start_k6_steady_bg() {
  local users="${USERS:-20}"
  local logfile="${1:-/tmp/chaos-k6.log}"
  cd "$REPO_ROOT"
  # shellcheck disable=SC2086
  nohup k6 run \
    -e "SCENARIO=steady" \
    -e "USERS=${users}" \
    -e "BASE_URL=${BASE_URL}" \
    --duration "${K6_DURATION:-30s}" \
    tests/load/transfers.js >"$logfile" 2>&1 &
  echo $!
}
