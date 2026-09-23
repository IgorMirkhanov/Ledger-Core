# Промпт 05: Accounts use cases + gRPC transport + фоновые воркеры

**Контекст:** @docs/database.md (§1.4 алгоритмы) @docs/events.md @internal/accounts @internal/platform/idempotency @internal/platform/outbox @api/proto/ledger/accounts/v1/accounts.proto @.cursor/rules/20-money-and-ledger.mdc

---

## Часть A: service (`internal/accounts/service/service.go`)
Реализуй все методы, сейчас возвращающие `ErrNotImplemented`, строго по алгоритмам `docs/database.md` §1.4.

Общий шаблон мутирующего use case (вынеси в хелпер `s.runIdempotent(ctx, scope, idem, fn)`):
```
tx.WithTx:
  rec := idem.Begin(scope, key, hash)
  if rec != nil → json.Unmarshal(rec.Response) → return (replay; инкремент метрики idempotency_replays_total)
  result := fn(ctx, tx)                 // бизнес-логика
  idem.Complete(scope, key, json(result), "OK")
```
- Scope = `"accounts.<Method>"`.
- Бизнес-ошибки (`INSUFFICIENT_FUNDS` и т.п.) **не** сохраняются как ответ: транзакция откатывается,
  ключ исчезает, клиент может повторить после пополнения. (Задокументируй это поведение в docs/api.md.)
- Проверка владельца (`CheckOwner`) для клиентских методов. Для `CreateHold/CaptureHold/ReleaseHold` и
  `GetAccount` с `x-caller=transfers` проверка владельца не выполняется (внутренний вызов; см. ADR-0008).
- CreateHold: при `ErrHoldReferenceExists` вернуть существующий холд, если amount совпадает, иначе `ErrValidation`.
- CaptureHold: порядок `Unreserve` → `Apply`. dest.currency должен совпадать с `DestAmount.Currency` → иначе `ErrCurrencyMismatch`.
  Если валюты source и dest различаются — 4 проводки через `fx.<CUR>` (`domain.TransferPostings`).
- ReleaseHold: `released`/`expired` → вернуть холд без изменений (идемпотентно); `captured` → `ErrHoldNotActive`.
- События outbox — по `docs/events.md` (topic `ledger.accounts.v1`, key = account_id или hold_id).
- `ExpireHolds(batch)`: `LockExpiredHolds` → для каждого `LockAccounts` → `Unreserve` → `Expire` → outbox `hold.released{reason:"expired"}`.

## Часть B: воркеры (`internal/accounts/worker/`)
- `HoldExpirer` (app.Runner): каждые 5s вызывает `ExpireHolds(100)`, пока возвращает 100 — без паузы.
- `Janitor` (app.Runner): раз в 10 мин чистит `idempotency_keys` и опубликованный `outbox` старше `OUTBOX_RETENTION`.
- Подключи в `cmd/accounts/main.go` вместе с `repository.New()` (закрой TODO).

## Часть C: gRPC (`internal/accounts/transport/grpc.go`)
- Реализуй все RPC `AccountsServiceServer`: валидация формата (UUID, currency, amount > 0, page_size 1..200) → `domain.ErrValidation`,
  чтение metadata через `grpcx.OwnerID/IdempotencyKey/Caller`, `idempotency.ValidateKey`, `idempotency.HashRequest(req)`,
  вызов service, маппинг domain → proto, ошибки через `grpcx.ToStatus(err, ErrorDomain, Errors)`.
- Маппинг proto ↔ domain вынеси в `transport/mapping.go`.
- `page_token` = base64url(posting_id).

## Тесты
- Unit (service с фейками портов, `service/fakes_test.go`): replay возвращает тот же результат и не вызывает repo второй раз;
  бизнес-ошибка не вызывает `Complete`; CaptureHold строит 4 проводки при FX.
- Integration (`tests/integration/accounts_service_test.go`, реальный PG):
  - Deposit → баланс, выписка, событие в outbox.
  - Deposit дважды с тем же ключом → одна проводка.
  - Withdraw больше доступного → `INSUFFICIENT_FUNDS`, балансы не изменились.
  - Hold → Capture: source −, dest +, held = 0. Hold → Release: доступный остаток восстановлен.
  - Capture истёкшего холда → `HOLD_EXPIRED`. ExpireHolds освобождает резерв.
  - **Главный тест на конкурентность** `TestConcurrentTransfers_PreserveTotal`: 10 счетов по 10 000, 1000 горутин делают
    Hold+Capture между случайными парами на случайные суммы. После: сумма балансов клиентов = 100 000,
    ни один баланс < 0, все held = 0, `SUM(balance)` по всем счетам валюты = 0, ни одного дедлока.
- gRPC-тест через `bufconn`: ошибки содержат `ErrorInfo.reason`.

Коммит(ы): `feat(accounts): use cases`, `feat(accounts): grpc transport`, `test(accounts): concurrency invariants`.
