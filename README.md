# Ledger Core

[![CI](https://github.com/IgorMirkhanov/Ledger-Core/actions/workflows/ci.yml/badge.svg)](https://github.com/IgorMirkhanov/Ledger-Core/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)

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
scripts/demo.sh         # токен → счета → депозит → перевод USD→RUB → выписка (после этапа 3)
make reconcile          # сверка инвариантов
```

| UI | URL |
|----|-----|
| REST API | http://localhost:8080/v1 |
| Jaeger | http://localhost:16686 |
| Grafana | http://localhost:3000 |
| Redpanda Console | http://localhost:8088 |
| Prometheus | http://localhost:9090 |

Разработка: `make help`, `make test` (unit + race), `make test-integration` (Docker), `make lint`, `make proto`.

## Документация

| Документ | О чём |
|----------|-------|
| [architecture.md](docs/architecture.md) | Сервисы, потоки, гарантии, слои |
| [database.md](docs/database.md) | Схема, эталонные запросы, алгоритмы операций |
| [saga.md](docs/saga.md) | Машина состояний перевода, обработка ошибок |
| [events.md](docs/events.md) | Kafka-топики и события |
| [api.md](docs/api.md) | REST-контракт |
| [adr/](docs/adr) | Архитектурные решения и альтернативы |
| [roadmap.md](docs/roadmap.md) | Этапы |

## Статус

🚧 В разработке. Этап 0 (каркас) готов: архитектура, схема БД с инвариантами на уровне Postgres,
gRPC-контракты, доменный слой с тестами, платформа (outbox relay, idempotency, graceful shutdown), CI.
План: [docs/roadmap.md](docs/roadmap.md).
