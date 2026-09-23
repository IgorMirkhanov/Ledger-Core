# Архитектура Ledger Core

> Документ-источник истины. Если код расходится с этим документом, прав документ,
> пока его явно не изменили (через ADR в `docs/adr`).

## 1. Цель системы

Ledger Core — упрощённое ядро банковского процессинга:

- мультивалютные счета клиентов;
- пополнение и вывод средств (эмуляция внешнего мира через системные счета);
- переводы между счетами, в том числе с конвертацией валют;
- холдирование средств (hold → capture / release), как при карточной авторизации;
- выписки по счёту;
- уведомления о движении средств;
- ежедневная сверка (reconciliation).

**Главное нефункциональное требование: деньги не создаются и не исчезают.**
Ни при повторных запросах, ни при гонках, ни при падении любого сервиса в любой момент.

## 2. Гарантии (контракт системы)

| # | Гарантия | Как обеспечивается |
|---|----------|--------------------|
| G1 | Сумма проводок каждой журнальной записи в каждой валюте = 0 | Доменная валидация + deferred constraint trigger в Postgres |
| G2 | Сумма балансов всех счетов в каждой валюте = 0 | Следствие G1 + системные счета `settlement.*`, проверяется reconciler'ом |
| G3 | Доступный остаток клиентского счёта никогда не < 0 | `CHECK (allow_overdraft OR balance - held >= 0)` + `SELECT ... FOR UPDATE` |
| G4 | Повтор запроса с тем же `Idempotency-Key` не выполняет операцию повторно | Таблица `idempotency_keys` в той же транзакции, что и бизнес-операция |
| G5 | Событие публикуется в Kafka тогда и только тогда, когда закоммичена бизнес-транзакция | Transactional Outbox + relay (at-least-once) |
| G6 | Потребитель обрабатывает событие эффективно ровно один раз | Inbox-таблица `processed_events` (дедуп по `event_id`) |
| G7 | Перевод всегда приходит в терминальное состояние (`completed`/`failed`) | Saga + recovery worker + TTL у холдов |
| G8 | Проводки неизменяемы | Триггер, запрещающий `UPDATE`/`DELETE` на `postings` и `journal_entries` |
| G9 | Баланс = сумма проводок | `balance` — кэш, пересчитываемый в той же транзакции; reconciler проверяет |

## 3. Сервисы

```
                    ┌──────────────────────────┐
  HTTP/JSON ───────►│         gateway          │  JWT, rate limit (Redis),
                    │   REST → gRPC, problem+json│  request-id, Idempotency-Key
                    └───────┬──────────┬───────┘
                            │ gRPC     │ gRPC
                 ┌──────────▼───┐   ┌──▼──────────────┐
                 │   accounts   │◄──┤    transfers    │  saga orchestrator
                 │   (ledger)   │gRPC│ + recovery worker│  + FX rates
                 └──┬────────┬──┘   └──┬───────────┬──┘
                    │        │         │           │
               ┌────▼───┐ outbox   ┌───▼────┐   outbox
               │ PG     │  relay   │ PG     │    relay
               │accounts│    │     │transfers│     │
               └────────┘    ▼     └────────┘     ▼
                       ┌──────────────────────────────┐
                       │   Kafka (Redpanda локально)   │
                       └──────────────┬───────────────┘
                                      ▼
                             ┌─────────────────┐    ┌───────────┐
                             │  notifications  │───►│PG notific.│ inbox + уведомления
                             └─────────────────┘    └───────────┘

   reconciler (cron job) ──► читает PG accounts (+ gRPC transfers) ──► отчёт + метрики
```

| Сервис | Ответственность | Хранилище | Входы | Выходы |
|--------|-----------------|-----------|-------|--------|
| `gateway` | Аутентификация (JWT), rate limit, валидация, маппинг REST↔gRPC, ошибки RFC 7807 | Redis | HTTP :8080 | gRPC к accounts, transfers |
| `accounts` | **Единственный владелец денег.** Счета, журнал проводок, холды, выписки | PG `accounts` | gRPC :9091 | Kafka `ledger.accounts.v1` |
| `transfers` | Оркестрация переводов (saga), FX-курсы, восстановление зависших переводов | PG `transfers` | gRPC :9092 | gRPC к accounts, Kafka `ledger.transfers.v1` |
| `notifications` | Потребляет события, дедуплицирует, «отправляет» уведомления (лог + таблица) | PG `notifications` | Kafka | — |
| `reconciler` | Проверка инвариантов G1, G2, G3, G9 и межсервисной согласованности | read-only PG `accounts`, gRPC transfers | cron / CLI | метрики, таблица отчётов |

Каждый сервис владеет своей БД (**database per service**). Никаких cross-DB запросов,
кроме read-only доступа reconciler'а к `accounts` (осознанное исключение, см. ADR-0007).

## 4. Ключевые потоки

### 4.1 Пополнение (Deposit)

```
gateway ── Deposit(account, amount, Idempotency-Key) ──► accounts
accounts, одна транзакция:
  1. INSERT idempotency_keys (scope='accounts.Deposit', key)  -- при конфликте вернуть сохранённый ответ
  2. SELECT accounts WHERE id IN (client, settlement.<CUR>) ORDER BY id FOR UPDATE
  3. INSERT journal_entries(kind='deposit')
  4. INSERT postings: settlement.<CUR> −amount; client +amount
  5. UPDATE accounts SET balance = balance ± amount, version = version + 1
  6. INSERT outbox(event_type='account.credited')
  7. UPDATE idempotency_keys SET response = ...
COMMIT
```

### 4.2 Перевод (saga, подробно в `docs/saga.md`)

```
created ──HoldFunds──► funds_held ──CaptureHold──► completed
   │                      │
   │ бизнес-отказ         │ бизнес-отказ при capture / hold expired
   ▼                      ▼
 failed ◄──ReleaseHold── compensating
```

1. `transfers` в одной транзакции создаёт `transfer(status=created)` + idempotency key + outbox `transfer.created`.
2. Шаг **HoldFunds**: `accounts.CreateHold(key="transfer:<id>:hold")`. Успех: `funds_held`. Недостаточно средств: `failed`.
3. Шаг **Settle**: `accounts.CaptureHold(key="transfer:<id>:capture", dest, dest_amount)`.
   Accounts атомарно закрывает холд и пишет проводки (с FX-ногами, если валюты разные). Успех: `completed`.
4. Бизнес-ошибка на шаге 3: `compensating` → `ReleaseHold` → `failed`.
5. Транзиентная ошибка (таймаут, `Unavailable`): шаг не меняет статус, выставляется `next_attempt_at`
   с экспоненциальным backoff. **Recovery worker** подбирает такие переводы.
6. Страховка: холд имеет TTL (15 мин). Если saga «потерялась», холд истечёт и деньги вернутся;
   capture истёкшего холда вернёт `FAILED_PRECONDITION`, и saga уйдёт в компенсацию.

Все вызовы из `transfers` в `accounts` идемпотентны за счёт **детерминированных ключей**
`transfer:<id>:<step>`, поэтому повтор шага после таймаута безопасен.

### 4.3 Уведомления

`outbox relay` → Kafka (key = `aggregate_id`) → `notifications` consumer:
`BEGIN; INSERT processed_events(event_id) ON CONFLICT DO NOTHING; если вставилось, создать notification; COMMIT; commit offset`.
После N неудачных попыток событие уходит в `ledger.notifications.dlq`.

## 5. Модель денег

- Сумма — `int64` в **минорных единицах** (копейки, центы). `float` запрещён везде, включая JSON:
  в API суммы передаются **строкой минорных единиц** (`"amount": "10050"`) + `currency`.
- Валюта — ISO 4217 (`RUB`, `USD`, `EUR`, `JPY`…), экспонента берётся из таблицы `currencies`.
- Курс FX — `numeric(20,10)` в БД, `*big.Rat` в Go. Округление при конвертации — **banker's rounding (half-even)**.
- Пакет `internal/money` — единственное место, где разрешена арифметика над суммами (с проверкой переполнения).

## 6. Двойная запись

- **Журнальная запись** (`journal_entries`) — бизнес-операция (депозит, перевод, FX, reversal).
- **Проводка** (`postings`) — изменение баланса одного счёта: `amount` со знаком
  (`+` увеличивает баланс счёта, `−` уменьшает).
- Инвариант: `SUM(amount) GROUP BY entry_id, currency = 0`, минимум 2 проводки.
- Внешний мир представлен системными счетами `settlement.<CUR>` (может уходить в минус:
  минус на нём означает «деньги, которые пришли извне»). Для FX — счета `fx.<CUR>`,
  для комиссий — `fee.<CUR>`.
- Кросс-валютный перевод 100 USD → RUB по курсу 90:
  ```
  client_usd   −10000 USD
  fx.USD       +10000 USD
  fx.RUB     −900000 RUB
  client_rub  +900000 RUB
  ```
  Сумма по USD = 0, сумма по RUB = 0.

## 7. Конкурентность

- Уровень изоляции: **READ COMMITTED + явные блокировки строк** (`SELECT ... FOR UPDATE`).
  Обоснование в ADR-0003.
- Блокировка счетов всегда в **порядке возрастания `id`**, что исключает дедлоки при встречных переводах.
- Холд блокирует свою строку `holds` + строку счёта.
- Горячие системные счета (`settlement.*`, `fx.*`): известное узкое место.
  Решение на будущее описано в ADR-0003 (шардирование системного счёта на N суб-счетов).

## 8. Надёжность и отказоустойчивость

| Механизм | Где |
|----------|-----|
| Таймауты через `context` на каждом внешнем вызове | везде; gRPC deadline из gateway = 5s |
| Ретраи с экспоненциальным backoff + jitter, только для транзиентных кодов | transfers → accounts, relay → Kafka |
| Circuit breaker (`sony/gobreaker`) | transfers → accounts, gateway → downstream |
| Graceful shutdown: stop accepting → drain → close pools (таймаут 15s) | `internal/platform/app` |
| Health: `/healthz` (liveness), `/readyz` (PG + Kafka/Redis ping) | каждый сервис, HTTP admin-порт |
| Outbox relay: один лидер на сервис через `pg_try_advisory_lock` | `internal/platform/outbox` |

## 9. Наблюдаемость

- **Логи**: `log/slog` JSON, обязательные поля `service`, `trace_id`, `request_id`.
- **Трейсинг**: OpenTelemetry (OTLP → otel-collector → Jaeger). Контекст пробрасывается
  HTTP → gRPC → outbox (`headers.traceparent`) → Kafka → consumer.
- **Метрики** (Prometheus, admin-порт `/metrics`):
  - RED для HTTP/gRPC: `rpc_requests_total`, `rpc_duration_seconds` (histogram);
  - `ledger_transfers_total{status}`, `ledger_transfer_duration_seconds`;
  - `outbox_pending_events`, `outbox_publish_lag_seconds`;
  - `ledger_reconciliation_discrepancies{check}`;
  - `idempotency_replays_total`.
- **Grafana**: дашборд `deploy/grafana/dashboards/ledger.json`.

## 10. Порты (локально)

| Сервис | Порт | Назначение |
|--------|------|-----------|
| gateway | 8080 | public REST |
| gateway | 8081 | admin: /healthz /readyz /metrics |
| accounts | 9091 / 8091 | gRPC / admin |
| transfers | 9092 / 8092 | gRPC / admin |
| notifications | — / 8093 | admin |
| postgres | 5432 | БД `accounts`, `transfers`, `notifications` |
| redis | 6379 | |
| redpanda | 9092 → 19092 (host) | Kafka API |
| redpanda console | 8088 | UI топиков |
| jaeger | 16686 | UI трейсов |
| prometheus | 9090 | |
| grafana | 3000 | |

## 11. Структура репозитория

```
api/proto/ledger/{accounts,transfers}/v1/   gRPC-контракты (buf)
gen/                                         сгенерированный код (коммитится)
cmd/<service>/main.go                        точки входа: только wiring
internal/
  money/                                     Money, Currency, FX-конвертация
  platform/                                  переиспользуемая инфраструктура (без бизнес-логики)
    app/          lifecycle, graceful shutdown
    config/       загрузка env-конфига
    logger/       slog
    postgres/     pgxpool, WithTx, коды ошибок
    idempotency/  хранилище ключей
    outbox/       запись событий + relay
    kafka/        producer/consumer (franz-go)
    grpcx/        сервер/клиент, интерцепторы, маппинг ошибок
    httpx/        сервер, middleware, problem+json
    observability/ otel + prometheus
  accounts/{domain,repository,service,transport}
  transfers/{domain,repository,service,transport}
  gateway/
  notifications/
  reconciler/
migrations/<service>/                        goose SQL-миграции
deploy/                                      конфиги инфраструктуры для docker-compose
tests/{integration,load,chaos}/
docs/                                        архитектура, ADR, промпты для Cursor
```

### Правила слоёв (внутри сервиса)

```
transport (gRPC/HTTP) ──► service (use cases) ──► domain (чистые правила, без I/O)
                              │
                              └──► repository (интерфейс объявлен в service, реализация на pgx)
```

- `domain` не импортирует ничего, кроме `internal/money` и stdlib.
- `service` зависит от интерфейсов (`Repository`, `TxManager`, `Clock`, `IDGenerator`).
- `transport` не содержит бизнес-логики, только маппинг и валидацию формата.
- Сервисы **не импортируют** внутренние пакеты друг друга (`internal/accounts` ↛ `internal/transfers`).
  Общение только через gRPC/Kafka-контракты.
