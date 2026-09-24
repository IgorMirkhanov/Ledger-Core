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
| Completed iterations | ~20 000 |
| Dropped iterations (insufficient VUs / latency) | ~34 000 |
| Effective throughput | ~104 req/s |
| Errors (`http_req_failed`) | **0%** |
| POST /transfers p50 | ~1.60 s |
| POST /transfers p95 | ~3.30 s |
| POST /transfers p99 | ~3.96 s |
| Threshold `p(99)<300ms` | **not met** on this laptop/Docker setup |
| `make reconcile` after run | **0 discrepancies** |

### hot_account — 150 rps, 1 minute (80% → one dest)

| Metric | Value |
|--------|-------|
| Errors | ~30% (timeouts / contention under Docker Desktop) |
| POST /transfers p50 | ~1.45 s |
| POST /transfers p95 | ~3.24 s |
| Notes | Hot destination serializes on `SELECT … FOR UPDATE`; tail latency and failures rise vs steady |

### idempotent_retries — 50 rps, 1 minute

| Metric | Value |
|--------|-------|
| Errors | 0% |
| Idempotent replay checks | 100% pass (same `id` / status) |
| POST /transfers p95 | ~728 ms |

### read_mix — 200 rps, 1 minute (70% GET / 30% POST)

| Metric | Value |
|--------|-------|
| Errors | ~5.6% (mostly POST under residual load) |
| Overall p95 | ~455 ms |
| POST /transfers p95 | ~1.71 s |

### rate_limit — default limits, 200 rps, 10 seconds

| Metric | Value |
|--------|-------|
| 429 responses | 801 |
| `Retry-After` present | yes |
| Threshold `rate_limited count>0` | **pass** |

## Conclusions

1. **Correctness under load:** steady transfers finished with **0%** HTTP failures; reconciler reported **0** ledger discrepancies after the heavy run.
2. **Throughput ceiling on this machine:** ~100 sustained transfers/s with p50≈1.6 s — the 300 rps aspirational target and `p99<300ms` threshold are goals for a beefier deployment (more CPU for Go services / Postgres, connection pools, less Docker desktop overhead), not what a single laptop Docker stack delivers.
3. **Hot account:** contention shows up as longer p95/p99 and elevated errors when 80% of credits hit one account (`SELECT … FOR UPDATE` serialization).
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

## Native Linux baseline (no Docker Desktop)

To separate code limits from the laptop/Docker Desktop environment, the same stack was run natively on one
Linux VM: **4 vCPU, 15 GB RAM**, Postgres 16, Redis, `accounts` + `transfers` + `gateway` binaries and the load
generator all on the same machine (Kafka absent: the outbox accumulates, which does not affect the request path).
Rate limits disabled. Generator: `tests/load/gobench` (closed loop: a fixed number of requests in flight).
Each user owns two funded RUB accounts; transfers go between them in both directions.

| In flight | Transfers/s | p50 | p95 | p99 | Result |
|-----------|-------------|-----|-----|-----|--------|
| 1  | 72  | 13 ms  | 17 ms  | 21 ms  | 100% `completed` |
| 10 | 272 | 36 ms  | 48 ms  | 56 ms  | 100% `completed` |
| 25 | 312 | 78 ms  | 112 ms | 158 ms | 100% `completed` |
| 100 | 363–389 | 255–270 ms | 330–377 ms | 365–433 ms | 100% `completed` |

After ~25 800 transfers: G2 (sum of balances per currency = 0) holds, G9 (balance = sum of postings) has
0 mismatches, 0 active holds left, every transfer `completed`.

**Reading the numbers.**
- One transfer costs **~13 ms end to end** (gateway → transfers → 4 accounts RPCs → 6 short DB transactions).
- Throughput flattens at **~370 transfers/s**, and at that point CPU is ~90% busy across all processes
  (`accounts` ~70%, `transfers` ~55%, Postgres backends, gateway, the generator). The ceiling is the 4 vCPU box,
  not locks or queues in the code: latency grows linearly with requests in flight (Little's law), no errors appear.
- The Docker Desktop run above (~104 transfers/s, p50 1.6 s) is the same code: k6 capped at 200 VUs × 1.6 s ≈ 125 rps,
  and the latency comes from the VM and port-forwarding overhead of Docker Desktop on Windows.
- `p99 < 300 ms` holds up to ~310 transfers/s on this box (25 in flight).

```bash
docker compose -f docker-compose.yml -f docker-compose.load.yml up -d --build \
  postgres redis redpanda accounts transfers notifications gateway
for c in 1 10 25 100; do go run ./tests/load/gobench -users 100 -c $c -d 30s; done
docker compose run --rm reconciler
```

## Chaos

Scripts under `tests/chaos/` exercise failure recovery with the same load compose overlay
(`docker-compose.yml` + `docker-compose.load.yml`). Each script prints **PASS** or **FAIL**.

| Scenario | Script | Result (2026-09-24) | Expected outcome |
|----------|--------|---------------------|------------------|
| Kill accounts mid-load | `kill_accounts_mid_load.sh` | **PASS** | k6 steady briefly; accounts killed ~10s then started; seeded transfers reach `completed`/`failed`; `reconcile` = 0 discrepancies |
| Kill transfers mid-load | `kill_transfers.sh` | **PASS** | Same pattern for transfers; recovery worker advances in-flight sagas after restart; money consistent |
| Kafka / Redpanda down | `kafka_down.sh` | **PASS** | Transfer create still works while Redpanda is stopped; `outbox` pending rows grow; after start, pending drains to 0; reconcile clean |
| Postgres restart | `postgres_restart.sh` | **PASS** | Admin `/readyz` briefly 503 then 200; pre-created account balance unchanged; new transfer + reconcile succeed |

**PASS criterion:** ledger money stays consistent (G1/G2/G9 via reconciler) and every observed transfer reaches a terminal status. Transient HTTP/gRPC errors during the outage window are expected; lost money or stuck non-terminal transfers are not.

```bash
docker compose -f docker-compose.yml -f docker-compose.load.yml up -d --build \
  postgres redis redpanda accounts transfers notifications gateway
bash tests/chaos/kill_accounts_mid_load.sh
bash tests/chaos/kill_transfers.sh
bash tests/chaos/kafka_down.sh
bash tests/chaos/postgres_restart.sh
```
