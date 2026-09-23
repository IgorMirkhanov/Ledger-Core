#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE_URL:-http://localhost:8080/v1}"

token=$(curl -sf -X POST "$BASE/dev/token" -H 'Content-Type: application/json' -d '{}' | jq -r .token)
auth=(-H "Authorization: Bearer $token" -H "Content-Type: application/json")

rub=$(curl -sf -X POST "$BASE/accounts" "${auth[@]}" -H "Idempotency-Key: $(uuidgen)" -d '{"currency":"RUB"}' | jq -r .id)
usd=$(curl -sf -X POST "$BASE/accounts" "${auth[@]}" -H "Idempotency-Key: $(uuidgen)" -d '{"currency":"USD"}' | jq -r .id)

curl -sf -X POST "$BASE/accounts/$usd/deposits" "${auth[@]}" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":"10000","currency":"USD"}' >/dev/null

tr=$(curl -sf -X POST "$BASE/transfers" "${auth[@]}" -H "Idempotency-Key: $(uuidgen)" \
  -d "{\"source_account_id\":\"$usd\",\"dest_account_id\":\"$rub\",\"amount\":\"10000\",\"currency\":\"USD\",\"dest_currency\":\"RUB\"}")
echo "$tr" | jq .

curl -sf "$BASE/accounts/$rub/statement" -H "Authorization: Bearer $token" | jq .
echo "demo ok: RUB=$rub USD=$usd"
