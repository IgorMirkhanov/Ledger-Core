# Benchmarks

> All ledger throughput numbers below were measured with **rate limits disabled**
> (`docker-compose.load.yml`: `RATE_LIMIT_IP_RPS=100000`, `RATE_LIMIT_RPS=100000`).
> The separate `rate_limit` scenario runs against **default** limits and proves 429s still work.

## Hardware / environment

| Item | Value |
|------|-------|
| Host | Windows 10, Docker Desktop (WSL2/linux engine) |
| Stack | `docker compose -f docker-compose.yml -f docker-compose.load.yml` (postgres, redis, redpanda, accounts, transfers, notifications, gateway) |
| Client | k6 v1.0.0 on the host → `http://localhost:8080` |
| Date | 2026-09-24 |

Observability sidecars (Grafana/Prometheus/Jaeger/otel) were not required for the run; screenshots are omitted when those images are unavailable.

## Results

### steady — 300 rps target, 3 minutes, 100 users

| Metric | Value |
|--------|-------|
| Target arrival rate | 300 transfers/s |
| Completed iterations | ~35 000 |
| Dropped iterations (insufficient VUs / latency) | ~19 000 |
| Effective throughput | ~190 req/s |
| Errors (`http_req_failed`) | **0%** |
| POST /transfers p50 | ~990 ms |
| POST /transfers p95 | ~1.26 s |
| POST /transfers p99 | ~1.61 s |
| Threshold `p(99)<300ms` | **not met** on this laptop/Docker setup |
| `make reconcile` after run | **0 discrepancies** |

### hot_account — 150 rps, 1 minute (80% → one dest)

| Metric | Value |
|--------|-------|
| Errors | 0% |
| POST /transfers p50 | ~1.21 s |
| POST /transfers p95 | ~1.91 s |
| Notes | Higher tail latency: row locks on the hot destination account |

### idempotent_retries — 50 rps, 1 minute

| Metric | Value |
|--------|-------|
| Errors | 0% |
| Idempotent replay checks | 100% pass (same `id` / status) |
| POST /transfers p95 | ~108 ms |

### read_mix — 200 rps, 1 minute (70% GET / 30% POST)

| Metric | Value |
|--------|-------|
| Errors | 0% |
| Overall p95 | ~93 ms |
| POST /transfers p95 | ~114 ms |

### rate_limit — default limits, 200 rps, 10 seconds

| Metric | Value |
|--------|-------|
| 429 responses | 802 |
| `Retry-After` present | yes |
| Threshold `rate_limited count>0` | **pass** |

## Conclusions

1. **Correctness under load:** zero HTTP failures on transfer scenarios; reconciler reported **0** ledger discrepancies after the heavy steady run.
2. **Throughput ceiling on this machine:** ~190 sustained transfers/s with p50≈1 s — the 300 rps aspirational target and `p99<300ms` threshold are goals for a beefier deployment (more CPU for Go services / Postgres, connection pools, less Docker desktop overhead), not what a single laptop Docker stack delivers.
3. **Hot account:** contention shows up as longer p95/p99 when 80% of credits hit one account (`SELECT … FOR UPDATE` serialization).
4. **Idempotency:** replays with the same key return the same transfer id under concurrent load.
5. **Rate limiter:** with default `RATE_LIMIT_IP_RPS`, junk-token bursts correctly receive **429** + `Retry-After`; load/chaos must keep using `docker-compose.load.yml`.

## How to reproduce

```bash
# ledger numbers (limits off)
docker compose -f docker-compose.yml -f docker-compose.load.yml up -d --build \
  postgres redis redpanda accounts transfers notifications gateway
k6 run -e SCENARIO=steady tests/load/transfers.js
docker compose run --rm reconciler

# limiter proof (defaults)
docker compose up -d gateway   # recreate without load override
k6 run tests/load/rate_limit.js
```

Or `make load` / `make load-rate-limit` (requires `k6` on `PATH`).
