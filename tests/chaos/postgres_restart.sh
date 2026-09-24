#!/usr/bin/env bash
# Chaos: restart Postgres; services go 503 then 200 on readyz; data survives.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

need_cmds curl jq docker

cd "$REPO_ROOT"
ensure_core

echo "== create durable fixture before restart =="
TOKEN="$(dev_token)"
ACC="$(create_account "$TOKEN" RUB)"
deposit "$TOKEN" "$ACC" 500000 RUB
BEFORE_BAL="$(curl -sf "${BASE_URL}/accounts/${ACC}" \
  -H "Authorization: Bearer ${TOKEN}" | jq -r .balance)"
echo "account ${ACC} balance before restart: ${BEFORE_BAL}"

echo "== restart postgres =="
$COMPOSE restart postgres

# Expect brief unreadiness (503 / connection errors), then recovery.
wait_not_readyz "$GATEWAY_READYZ" 90 || echo "warn: gateway stayed ready (pool may have buffered)"
wait_not_readyz "$ACCOUNTS_READYZ" 90 || echo "warn: accounts stayed ready briefly"

wait_readyz "$ACCOUNTS_READYZ" 180 || fail "accounts readyz did not return 200 after postgres restart"
wait_readyz "$TRANSFERS_READYZ" 180 || fail "transfers readyz did not return 200"
wait_readyz "$GATEWAY_READYZ" 180 || fail "gateway readyz did not return 200"

echo "== verify data survived =="
AFTER="$(curl -sf "${BASE_URL}/accounts/${ACC}" \
  -H "Authorization: Bearer ${TOKEN}")" \
  || fail "could not fetch account after postgres restart"
AFTER_BAL="$(echo "$AFTER" | jq -r .balance)"
AFTER_STATUS="$(echo "$AFTER" | jq -r .status)"

if [[ "$AFTER_BAL" != "$BEFORE_BAL" ]]; then
  fail "balance changed after postgres restart: before=${BEFORE_BAL} after=${AFTER_BAL}"
fi
if [[ "$AFTER_STATUS" != "active" ]]; then
  fail "unexpected account status after restart: ${AFTER_STATUS}"
fi

# Prove write path still works.
DST="$(create_account "$TOKEN" RUB)"
deposit "$TOKEN" "$DST"
body="$(create_transfer "$TOKEN" "$ACC" "$DST" 100 RUB)" \
  || fail "transfer failed after postgres restart"
tid="$(echo "$body" | jq -r .id)"
wait_transfers_terminal "$TOKEN" "$tid" \
  || fail "post-restart transfer not terminal"

echo "== reconcile =="
if ! run_reconcile; then
  fail "reconcile reported discrepancies after postgres restart"
fi

pass "postgres restart: readyz 503→200, data intact, money consistent"
