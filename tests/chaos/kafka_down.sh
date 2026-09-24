#!/usr/bin/env bash
# Chaos: stop Redpanda; transfers continue via outbox; pending drains after Kafka returns.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

need_cmds curl jq docker

cd "$REPO_ROOT"
ensure_core
wait_readyz "$ACCOUNTS_READYZ" 60 || fail "accounts not ready"
wait_readyz "$TRANSFERS_READYZ" 60 || fail "transfers not ready"

pending_total() {
  local a t
  a="$(outbox_pending accounts)"
  t="$(outbox_pending transfers)"
  echo $((a + t))
}

BEFORE="$(pending_total)"
echo "outbox pending before Kafka down: accounts+transfers=${BEFORE}"
METRIC_BEFORE="$(outbox_pending_metric 8091 || true)"
[[ -n "${METRIC_BEFORE}" ]] && echo "accounts outbox_pending_events metric: ${METRIC_BEFORE}"

echo "== stop redpanda for 30s =="
$COMPOSE stop redpanda

TOKEN="$(dev_token)"
SRC="$(create_account "$TOKEN" RUB)"
DST="$(create_account "$TOKEN" RUB)"
deposit "$TOKEN" "$SRC"
deposit "$TOKEN" "$DST"

echo "== create transfer while Kafka is down (must succeed — money path independent of Kafka) =="
BODY="$(create_transfer "$TOKEN" "$SRC" "$DST" 100 RUB)" \
  || fail "transfer create failed while Kafka down (expected to continue)"
TID="$(echo "$BODY" | jq -r .id)"
echo "transfer while down: id=${TID} status=$(echo "$BODY" | jq -r .status)"

# Extra writes so pending has room to grow while relay is stuck.
for _ in 1 2 3; do
  create_transfer "$TOKEN" "$SRC" "$DST" 10 RUB >/dev/null || true
done

sleep 30

MID="$(pending_total)"
echo "outbox pending mid-outage: ${MID}"
if (( MID <= BEFORE )); then
  fail "expected outbox pending to grow while Kafka down (before=${BEFORE}, mid=${MID})"
fi
echo "note: transfers/API continued while Redpanda was stopped; outbox backlog grew"

echo "== start redpanda =="
$COMPOSE start redpanda
# Relay needs Kafka; services may briefly report not-ready then recover.
wait_readyz "$ACCOUNTS_READYZ" 180 || fail "accounts not ready after redpanda start"
wait_readyz "$TRANSFERS_READYZ" 180 || fail "transfers not ready after redpanda start"
wait_readyz "$GATEWAY_READYZ" 60 || true

echo "== wait for outbox pending to drain =="
DRAIN_TIMEOUT="${OUTBOX_DRAIN_TIMEOUT:-120}"
i=0
while (( i < DRAIN_TIMEOUT )); do
  NOW="$(pending_total)"
  echo "  pending=${NOW} (t=${i}s)"
  if (( NOW == 0 )); then
    break
  fi
  sleep 2
  i=$((i + 2))
done
NOW="$(pending_total)"
if (( NOW != 0 )); then
  fail "outbox pending did not drain to 0 (still ${NOW})"
fi

wait_transfers_terminal "$TOKEN" "$TID" \
  || fail "transfer ${TID} not terminal after Kafka recovery"

echo "== reconcile =="
if ! run_reconcile; then
  fail "reconcile reported discrepancies after kafka outage"
fi

pass "kafka down: transfers continued, outbox grew then drained, money consistent"
