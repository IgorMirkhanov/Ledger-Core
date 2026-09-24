#!/usr/bin/env bash
set -euo pipefail

need() { command -v "$1" >/dev/null 2>&1 || { echo "demo.sh: missing dependency: $1" >&2; exit 1; }; }
need curl
need jq

# uuidgen is not available in minimal Linux images; fall back to kernel UUID or openssl.
new_key() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  elif command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    echo "demo.sh: need uuidgen, /proc/sys/kernel/random/uuid, or openssl" >&2
    exit 1
  fi
}

BASE="${BASE_URL:-http://localhost:8080/v1}"

token=$(curl -sf -X POST "$BASE/dev/token" -H 'Content-Type: application/json' -d '{}' | jq -r .token)
auth=(-H "Authorization: Bearer $token" -H "Content-Type: application/json")

rub=$(curl -sf -X POST "$BASE/accounts" "${auth[@]}" -H "Idempotency-Key: $(new_key)" -d '{"currency":"RUB"}' | jq -r .id)
usd=$(curl -sf -X POST "$BASE/accounts" "${auth[@]}" -H "Idempotency-Key: $(new_key)" -d '{"currency":"USD"}' | jq -r .id)

curl -sf -X POST "$BASE/accounts/$usd/deposits" "${auth[@]}" -H "Idempotency-Key: $(new_key)" \
  -d '{"amount":"10000","currency":"USD"}' >/dev/null

tr=$(curl -sf -X POST "$BASE/transfers" "${auth[@]}" -H "Idempotency-Key: $(new_key)" \
  -d "{\"source_account_id\":\"$usd\",\"dest_account_id\":\"$rub\",\"amount\":\"10000\",\"currency\":\"USD\",\"dest_currency\":\"RUB\"}")
echo "$tr" | jq .

curl -sf "$BASE/accounts/$rub/statement" -H "Authorization: Bearer $token" | jq .
echo "demo ok: RUB=$rub USD=$usd"
