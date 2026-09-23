# Промпт 07: Transfers gRPC + end-to-end тесты accounts ↔ transfers

**Контекст:** @api/proto/ledger/transfers/v1/transfers.proto @internal/transfers @internal/accounts/transport @docs/saga.md

---

## 1. gRPC transport (`internal/transfers/transport/grpc.go`)
По образцу accounts: валидация, metadata (`x-owner-id`, `idempotency-key`), `HashRequest`, маппинг, `grpcx.ToStatus`.
`dest_currency` пустая → берётся валюта dest-счёта из `GetAccountInfo`. `currency` должна совпадать с валютой source.

## 2. E2E-тесты (`tests/integration/e2e_transfers_test.go`)
Поднять в одном процессе: PG (две БД), accounts gRPC server на `bufconn`, transfers service c реальным accountsclient.
- Перевод RUB→RUB: completed, балансы, 2 проводки, события в обоих outbox.
- Перевод USD→RUB по курсу 90: 4 проводки, G2 (сумма балансов по каждой валюте = 0).
- Недостаточно средств → failed, холдов нет.
- Dest заморожен (UPDATE accounts SET status='frozen' в тесте) → failed/ACCOUNT_NOT_ACTIVE, source восстановлен.
- **Chaos-in-process**: обёртка над accounts-клиентом, которая на capture выполняет вызов, но возвращает `Unavailable`
  (ответ «потерялся»). Recovery worker доводит → completed, ровно 1 journal entry.
- 200 параллельных переводов между 20 счетами (A→B и B→A вперемешку) → все терминальные, сумма сохранена.

Коммит: `feat(transfers): grpc transport` + `test: e2e transfers saga`.
