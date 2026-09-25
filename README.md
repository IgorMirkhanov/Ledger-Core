# Ledger Core

[![CI](https://github.com/IgorMirkhanov/Ledger-Core/actions/workflows/ci.yml/badge.svg)](https://github.com/IgorMirkhanov/Ledger-Core/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/IgorMirkhanov/Ledger-Core)](https://github.com/IgorMirkhanov/Ledger-Core/releases/latest)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![Coverage](https://img.shields.io/badge/coverage-internal-informational)

Ядро банковского процессинга на Go: мультивалютные счета, **журнал с двойной записью**, холды,
переводы через **saga** с компенсациями, **transactional outbox**, идемпотентность, сверка.

> Главная гарантия: **деньги не создаются и не исчезают** ни при повторных запросах,
> ни при гонках, ни при падении любого сервиса в любой момент.

## Архитектура

```
 client ──HTTP──► gateway ──gRPC──► accounts (ledger) ◄──gRPC── transfers (saga)
                  JWT, rate limit    PG + outbox              PG + outbox + recovery
                                          │                         │
                                          └──────► Kafka ◄──────────┘
                                                     │
                                                     ▼
                                              notifications (inbox)
                          reconciler (cron) ──► проверка инвариантов
```

Подробно: [docs/architecture.md](docs/architecture.md)

## Гарантии и как они обеспечены

| Гарантия | Механизм |
|----------|----------|
| Сумма проводок каждой операции = 0 | Домен + deferred constraint trigger в Postgres |
| Остаток клиента не уходит в минус | `SELECT … FOR UPDATE` в порядке id + CHECK constraint |
| Повтор запроса не выполняет операцию дважды | `Idempotency-Key` в той же транзакции, что и операция |
| Событие ⇔ закоммиченная транзакция | Transactional outbox + relay с leader election (advisory lock) |
| Потребитель обрабатывает событие один раз | Inbox (`processed_events`) |
| Перевод всегда завершается | Saga + recovery worker + TTL холда |
| Журнал неизменяем | Триггеры append-only |
| Баланс = сумма проводок | Reconciler (read-only, консистентный снапшот) |

## Стек

Go 1.26 · gRPC + buf · PostgreSQL 17 (pgx, goose) · Kafka (Redpanda, franz-go) · Redis ·
OpenTelemetry + Jaeger · Prometheus + Grafana · testcontainers-go · k6 · Docker Compose · GitHub Actions

## Быстрый старт

```bash
make up                 # всё окружение в docker compose
make logs
scripts/demo.sh         # токен → счета → депозит → перевод USD→RUB → выписка
make reconcile          # сверка инвариантов
make load               # k6 (лимиты сняты через docker-compose.load.yml) + reconcile
```

Демо-сценарий (`scripts/demo.sh`) после `make up`:

```text
POST /v1/dev/token → create RUB+USD → deposit USD → FX transfer → statement
demo ok: RUB=… USD=…
```

| UI | URL |
|----|-----|
| REST API | http://localhost:8080/v1 |
| Jaeger | http://localhost:16686 |
| Grafana | http://localhost:3000 |
| Redpanda Console | http://localhost:8088 |
| Prometheus | http://localhost:9090 |

Готовые образы каждого сервиса публикуются в GHCR при релизе, подписаны cosign, с SBOM и provenance:
`ghcr.io/igormirkhanov/ledger-core-{gateway,accounts,transfers,notifications,reconciler}:0.1.0`
([релиз](https://github.com/IgorMirkhanov/Ledger-Core/releases/latest), выкладка в Kubernetes: [operations.md](docs/operations.md)).

Разработка: `make help`, `make test`, `make test-integration`, `make lint`, `make proto`.

## Нагрузка (кратко)

Linux VM, 4 vCPU, весь стек на одной машине (лимиты **отключены**): **~13 мс** на перевод без очереди,
**~310 переводов/с при p99 158 мс**, потолок ~370/с упирается в CPU; 0 ошибок, reconciler **0** расхождений
после ~25 800 переводов. На ноутбуке с Docker Desktop: ~100/с (накладные расходы Docker Desktop). Подробности и сценарии chaos: [docs/benchmarks.md](docs/benchmarks.md).

## Документация

| Документ | О чём |
|----------|-------|
| [architecture.md](docs/architecture.md) | Сервисы, потоки, гарантии, слои |
| [database.md](docs/database.md) | Схема, эталонные запросы, алгоритмы операций |
| [saga.md](docs/saga.md) | Машина состояний перевода, обработка ошибок |
| [events.md](docs/events.md) | Kafka-топики и события |
| [api.md](docs/api.md) | REST-контракт |
| [benchmarks.md](docs/benchmarks.md) | k6 и chaos |
| [operations.md](docs/operations.md) | Выкладка и runbook'и к алертам |
| [production-readiness.md](docs/production-readiness.md) | Что готово к проду и что должна дать платформа |
| [interview-notes.md](docs/interview-notes.md) | Вопросы на собеседовании |
| [adr/](docs/adr) | Архитектурные решения |

## Статус

Реализованы сервисы gateway, accounts, transfers, notifications, reconciler; интеграционные и
нагрузочные сценарии; chaos-скрипты в `tests/chaos/`.
