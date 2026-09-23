# Промпт 06: Transfers — repository, accounts client, saga, recovery worker

**Контекст:** @docs/saga.md @docs/database.md (§2) @internal/transfers @api/proto @internal/platform/grpcx

---

## 1. Repository (`internal/transfers/repository/postgres.go`)
Реализуй `service.Repository`. `Update` — оптимистичный (`WHERE id=$1 AND version=$2`, где `$2 = t.Version-1`),
0 строк → `service.ErrConcurrentUpdate` (добавь sentinel). `fx_rate` ↔ `*big.Rat` через строку (`rat.FloatString(10)` при записи,
`SetString` при чтении). `ClaimPending` — запрос из `docs/database.md` §2. `ListByOwner` — keyset по `(created_at, id)`.

## 2. Accounts client (`internal/transfers/accountsclient/client.go`)
Реализует `service.AccountsClient` поверх `accountsv1.AccountsServiceClient`:
- metadata: `idempotency-key`, `x-caller: transfers`, `x-request-id` из ctx; `x-owner-id` — только в `GetAccountInfo`
  для source-счёта (accounts проверит владельца). Для dest и для операций с холдами `x-owner-id` не передаётся;
- ключ идемпотентности `CreateTransfer` в БД transfers: `idempotency.Namespace(ownerID.String(), key)`, replay → `idempotency.MarkReplayed(ctx)`
  (transport ставит заголовок `idempotent-replayed`, как в accounts);
- таймаут на вызов 3s (если у ctx нет более раннего deadline);
- ошибка gRPC с кодом из списка «бизнес-отказ» в `docs/saga.md` → `*service.BusinessError{Code: grpcx.Reason(err)}`; остальное → wrap как транзиентную;
- circuit breaker `github.com/sony/gobreaker/v2` (открывается после 5 подряд транзиентных, полуоткрыт через 5s) — открытый breaker = транзиентная ошибка;
- `grpc.NewClient` с `otelgrpc` (если сделан промпт 03) и keepalive.

## 3. Saga (`internal/transfers/service/service.go`)
Реализуй `CreateTransfer`, `Advance`, `GetTransfer`, `ListTransfers` **строго** по алгоритмам `docs/saga.md`:
- В `Advance` НЕ держать транзакцию/блокировку во время gRPC-вызова (tx1 → вызов → tx2 со сверкой версии).
- `transfer_steps` пишется на каждый вызов accounts.
- События outbox `transfer.created/completed/failed` (topic `ledger.transfers.v1`, key = transfer_id).
- `CreateTransfer` крутит `Advance`, пока есть время: `deadline - 500ms`.
- `ErrConcurrentUpdate` в tx2 → перечитать и вернуть актуальное состояние (другой исполнитель уже продвинул).
- После 20 попыток на шаге release: `failure_code=MANUAL_REVIEW`, статус `failed`, лог ERROR, метрика `ledger_saga_anomalies_total`.
- Метрики: `ledger_transfers_total{status}` (при терминальном статусе), `ledger_transfer_duration_seconds` (created → terminal).

## 4. Recovery worker (`internal/transfers/worker/recovery.go`)
app.Runner: тикер `RECOVERY_INTERVAL` → `ClaimPending(now, 50)` (в короткой tx, только id) → `Advance` каждого с semaphore(10).
Подключи всё в `cmd/transfers/main.go` (закрой TODO).

## Тесты
- Unit с фейковым AccountsClient (программируемые ответы по шагам) — ВСЕ 10 сценариев из `docs/saga.md` §«Сценарии для тестов».
  Для сценария 10 — две горутины `Advance` одного перевода, фейк считает вызовы по ключам.
- Integration: repository (optimistic update, ClaimPending SKIP LOCKED из двух tx).

Коммиты: `feat(transfers): repository`, `feat(transfers): accounts grpc client with circuit breaker`, `feat(transfers): saga orchestrator and recovery worker`.
