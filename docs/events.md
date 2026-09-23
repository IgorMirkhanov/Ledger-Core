# События (Kafka)

## Топики

| Топик | Продюсер | Ключ | Партиций (локально) | Потребители |
|-------|----------|------|---------------------|-------------|
| `ledger.accounts.v1` | accounts (outbox relay) | `account_id` / `hold_id` | 3 | notifications |
| `ledger.transfers.v1` | transfers (outbox relay) | `transfer_id` | 3 | notifications |
| `ledger.notifications.dlq` | notifications | исходный ключ | 1 | человек / replay-скрипт |

Ключ = `aggregate_id`, поэтому события одного агрегата упорядочены внутри партиции.

## Гарантии доставки

- Продюсер: at-least-once (outbox relay + идемпотентный продюсер, `acks=all`).
- Потребитель: effectively-once за счёт inbox (`processed_events`).
- Offset коммитится **после** COMMIT транзакции в БД потребителя.
- Порядок: гарантирован для одного агрегата (один лидер relay на сервис + ключ партиционирования).

## Конверт (envelope), JSON

```json
{
  "event_id": "0192f7a4-6b1e-7c3d-9a2b-1f2e3d4c5b6a",
  "event_type": "account.credited",
  "schema_version": 1,
  "aggregate_type": "account",
  "aggregate_id": "0192f7a4-...",
  "occurred_at": "2026-09-23T10:00:00Z",
  "producer": "accounts",
  "data": { }
}
```

Kafka headers: `traceparent` (W3C), `event_type`.

## Каталог событий (`data`)

Все суммы: целые числа в минорных единицах.

### accounts

| event_type | Когда | data |
|------------|-------|------|
| `account.opened` | CreateAccount | `{account_id, owner_id, currency}` |
| `account.credited` | Deposit, Capture (dest) | `{account_id, owner_id, entry_id, amount, currency, balance_after, entry_kind}` |
| `account.debited` | Withdraw, Capture (source) | то же |
| `hold.created` | CreateHold | `{hold_id, account_id, owner_id, amount, currency, reference_id, expires_at}` |
| `hold.captured` | CaptureHold | `{hold_id, account_id, entry_id}` |
| `hold.released` | ReleaseHold / expirer | `{hold_id, account_id, reason: "released"\|"expired"}` |

### transfers

| event_type | data |
|------------|------|
| `transfer.created` | `{transfer_id, owner_id, source_account_id, dest_account_id, amount, currency, dest_amount, dest_currency}` |
| `transfer.completed` | `{transfer_id, owner_id, journal_entry_id, amount, currency, dest_amount, dest_currency}` |
| `transfer.failed` | `{transfer_id, owner_id, failure_code, failure_reason}` |

## Эволюция схем

- Добавление полей обратно совместимо, потребители обязаны игнорировать неизвестные поля.
- Несовместимое изменение: новый `event_type` или `schema_version`, либо новый топик `*.v2`.
- Следующий шаг (в README как «что бы я сделал»): Schema Registry + Protobuf-схемы событий.

## Уведомления: маппинг

| Событие | Кому | Шаблон |
|---------|------|--------|
| `account.credited` (entry_kind=deposit) | owner | `deposit_received` |
| `transfer.completed` | owner источника | `transfer_sent` |
| `account.credited` (entry_kind=transfer) | owner dest | `transfer_received` |
| `transfer.failed` | owner | `transfer_failed` |
