#!/usr/bin/env bash
# Chaos: kill transfers mid-load; recovery worker finishes saga after restart.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

need_cmds curl jq docker
command -v k6 >/dev/null 2>&1 || fail "missing dependency: k6"

cd "$REPO_ROOT"
ensure_core
wait_readyz "$TRANSFERS_READYZ" 60 || fail "transfers not ready before load"

echo "== seed transfer IDs to poll after recovery =="
TOKEN="$(dev_token)"
SRC="$(create_account "$TOKEN" RUB)"
DST="$(create_account "$TOKEN" RUB)"
deposit "$TOKEN" "$SRC"
deposit "$TOKEN" "$DST"

SEED_IDS=()
for _ in 1 2 3 4 5; do
  body="$(create_transfer "$TOKEN" "$SRC" "$DST" 50 RUB)"
  SEED_IDS+=("$(echo "$body" | jq -r .id)")
done
echo "seeded transfers: ${SEED_IDS[*]}"

K6_LOG="${TMPDIR:-/tmp}/chaos-k6-transfers.log"
rm -f "$K6_LOG"
echo "== start k6 steady (USERS=${USERS:-20}) in background =="
K6_PID="$(start_k6_steady_bg "$K6_LOG")"
echo "k6 pid=${K6_PID} log=${K6_LOG}"
trap 'kill "$K6_PID" 2>/dev/null || true' EXIT

sleep 5

echo "== kill transfers =="
$COMPOSE kill transfers
sleep 10

echo "== start transfers =="
$COMPOSE start transfers
wait_readyz "$TRANSFERS_READYZ" 120 || fail "transfers did not recover"
wait_readyz "$GATEWAY_READYZ" 60 || fail "gateway not ready after transfers restart"

kill "$K6_PID" 2>/dev/null || true
wait "$K6_PID" 2>/dev/null || true
trap - EXIT

# Post-recovery creates exercise the API; recovery worker advances in-flight sagas.
POST_IDS=()
for _ in 1 2 3; do
  body="$(create_transfer "$TOKEN" "$SRC" "$DST" 25 RUB || true)"
  id="$(echo "${body:-}" | jq -r .id 2>/dev/null || true)"
  if [[ -n "${id:-}" && "$id" != "null" ]]; then
    POST_IDS+=("$id")
  fi
done

ALL_IDS=("${SEED_IDS[@]}" "${POST_IDS[@]}")
wait_transfers_terminal "$TOKEN" "${ALL_IDS[@]}" \
  || fail "transfers did not reach terminal state after recovery: ${ALL_IDS[*]}"

echo "== reconcile =="
if ! run_reconcile; then
  fail "reconcile reported discrepancies after transfers kill"
fi

pass "transfers kill mid-load: recovery finished sagas, money consistent"
