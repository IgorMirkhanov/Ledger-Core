# Схема баз данных

Три базы, по одной на сервис: `accounts`, `transfers`, `notifications`.
Миграции: `migrations/<service>/*.sql` (goose), применяются при старте сервиса
(`POSTGRES_MIGRATE_ON_START=true`) или через `goose`.

**Правило:** миграции только добавляются. Изменить уже закоммиченную миграцию нельзя,
нужно написать новую.

---

## 1. БД `accounts` (ledger)

```
currencies 1───* accounts 1───* postings *───1 journal_entries
                    │                              ▲
                    1                              │ (captured hold → entry)
                    *                              │
                  holds ───────────────────────────┘

idempotency_keys   outbox   reconciliation_runs 1───* reconciliation_discrepancies
```

### 1.1 Таблицы

| Таблица | Назначение | Ключевые ограничения |
|---------|-----------|----------------------|
| `currencies` | ISO 4217 + экспонента | `code ~ '^[A-Z]{3}$'` |
| `accounts` | Счета клиентов и системные | `CHECK (allow_overdraft OR balance - held >= 0)`; customer ⇒ owner_id; system ⇒ code |
| `journal_entries` | Бизнес-операция | `UNIQUE (reference_type, reference_id, kind)`; append-only |
| `postings` | Изменение баланса одного счёта | `amount <> 0`; deferred-триггер «сумма по entry и валюте = 0»; валюта = валюте счёта; append-only |
| `holds` | Резерв средств | `UNIQUE (account_id, reference_id)`; captured ⇒ journal_entry_id |
| `idempotency_keys` | Ключи идемпотентности | `PK (scope, key)` |
| `outbox` | Исходящие события | partial index `WHERE published_at IS NULL` |
| `reconciliation_*` | Отчёты сверки | |

### 1.2 Системные счета

Создаются миграцией для каждой валюты: `settlement.<CUR>`, `fx.<CUR>`, `fee.<CUR>`
(`kind='system'`, `allow_overdraft=true`). Ищутся по `code`, id генерируется при миграции.

- `settlement.<CUR>` — «внешний мир». Депозит: `settlement −X, client +X`.
  Отрицательный баланс settlement = сколько денег клиенты завели в систему.
- `fx.<CUR>` — позиция по валюте при конвертации.
- `fee.<CUR>` — комиссии (зарезервировано на будущее).

**Инвариант G2:** `SELECT currency, SUM(balance) FROM accounts GROUP BY currency` всегда даёт 0.

### 1.3 Ключевые запросы (эталонные, используйте их в repository)

**Блокировка счетов (всегда в порядке id):**
```sql
SELECT id, kind, owner_id, code, currency, status, balance, held, allow_overdraft, version, created_at, updated_at
FROM accounts
WHERE id = ANY($1::uuid[])
ORDER BY id
FOR UPDATE;
```
> `ORDER BY id` + `FOR UPDATE` заставляет Postgres брать блокировки в порядке id,
> поэтому встречные переводы A→B и B→A не дедлочат.

**Обновление балансов (батчем):**
```sql
UPDATE accounts AS a
SET balance = v.balance, held = v.held, version = v.version, updated_at = v.updated_at
FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::bigint[]) AS balance,
             unnest($3::bigint[]) AS held, unnest($4::bigint[]) AS version,
             unnest($5::timestamptz[]) AS updated_at) AS v
WHERE a.id = v.id;
```

**Вставка проводок (одна вставка на всю запись):**
```sql
INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
SELECT $1, unnest($2::uuid[]), unnest($3::bigint[]), unnest($4::char(3)[]), unnest($5::bigint[]);
```

**Выписка (keyset-пагинация, без OFFSET):**
```sql
SELECT p.id, p.entry_id, e.kind, p.amount, p.balance_after, e.description, p.created_at
FROM postings p
JOIN journal_entries e ON e.id = p.entry_id
WHERE p.account_id = $1
  AND ($2::bigint = 0 OR p.id < $2)
  AND ($3::timestamptz IS NULL OR p.created_at >= $3)
  AND ($4::timestamptz IS NULL OR p.created_at <  $4)
ORDER BY p.id DESC
LIMIT $5;
```
`page_token` = base64(`last posting id`).

**Истёкшие холды (для hold expirer, несколько реплик параллельно):**
```sql
SELECT id, account_id, amount, status, reference_id, expires_at, journal_entry_id, created_at, updated_at
FROM holds
WHERE status = 'active' AND expires_at <= $1
ORDER BY expires_at
LIMIT $2
FOR UPDATE SKIP LOCKED;
```

### 1.4 Алгоритмы операций (одна транзакция на операцию)

**Deposit(account, amount, key):**
1. `idem.Begin(scope="accounts.Deposit", key, hash)`; replay → вернуть сохранённый ответ.
2. `settlement := GetSystemAccount("settlement", cur)`.
3. `LockAccounts([account, settlement])`.
4. Проверки: владелец, валюта, `CanCredit`.
5. Построить `JournalEntry{kind=deposit, reference_type="deposit", reference_id=key}`,
   postings `settlement −X`, `account +X`.
6. `entry.Apply(locked, now)`: валидирует G1, атомарно меняет балансы заблокированных счетов, заполняет `BalanceAfter`.
7. `InsertEntry`, `UpdateBalances`, `outbox.Add(account.credited)`.
8. `idem.Complete(response)`.

**Withdraw:** зеркально, `account −X`, `settlement +X`, событие `account.debited`.

**CreateHold(account, amount, reference, ttl, key):**
1. idem.Begin → 2. `LockAccounts([account])` → 3. `acc.Reserve(amount)` →
4. `INSERT holds` (при `holds_reference_uniq` конфликте вернуть существующий холд, если сумма совпадает) →
5. `UpdateBalances` → 6. outbox `hold.created` → 7. idem.Complete.

**CaptureHold(hold, dest, destAmount, key):**
1. idem.Begin.
2. `LockHold(hold)` (FOR UPDATE). Проверить `status=active`, не истёк.
3. Определить участников: `src = hold.account`, `dst`, при разных валютах `fx.<srcCur>`, `fx.<dstCur>`.
4. `LockAccounts(все участники)` (по id).
5. `src.Unreserve(hold.amount)`, затем `TransferPostings(...)` → `entry.Apply(locked, now)`.
   Порядок важен: сначала снять резерв, потом списать, иначе ложный `INSUFFICIENT_FUNDS`.
6. `hold.Capture(entryID)`, `UpdateHold`, `InsertEntry`, `UpdateBalances`.
7. outbox: `hold.captured`, `account.debited` (src), `account.credited` (dst).
8. idem.Complete.

**ReleaseHold / ExpireHolds:** `LockHold` → `LockAccounts([account])` → `Unreserve` → `hold.Release()/Expire()` → outbox `hold.released`.

---

## 2. БД `transfers`

| Таблица | Назначение |
|---------|-----------|
| `transfers` | Состояние saga (см. `docs/saga.md`), CHECK-и на согласованность статуса и полей |
| `transfer_steps` | Аудит каждого шага saga (outcome, error, duration) |
| `fx_rates` | Курсы. Курс фиксируется в переводе при создании |
| `idempotency_keys`, `outbox` | Как в accounts |

**Recovery worker (несколько реплик):**
```sql
SELECT id FROM transfers
WHERE status IN ('created', 'funds_held', 'compensating')
  AND next_attempt_at <= $1
ORDER BY next_attempt_at
LIMIT $2
FOR UPDATE SKIP LOCKED;
```
Далее для каждого id вызывается `Advance(id)` (в своей транзакции/вызове).

**Оптимистичное обновление:**
```sql
UPDATE transfers SET status=$2, hold_id=$3, ..., version=$N
WHERE id=$1 AND version=$N-1;
-- 0 rows affected → конкурентное изменение → перечитать и повторить шаг
```

---

## 3. БД `notifications`

| Таблица | Назначение |
|---------|-----------|
| `processed_events` | Inbox: `event_id` PK. Вставка и бизнес-действие в одной транзакции |
| `notifications` | Созданные уведомления (`pending → sent/failed`) |

Обработка события:
```sql
BEGIN;
INSERT INTO processed_events (event_id, event_type, topic, partition, "offset")
VALUES (...) ON CONFLICT (event_id) DO NOTHING;
-- 0 rows → дубль, COMMIT и commit offset
INSERT INTO notifications (...);
COMMIT;
-- после COMMIT: commit offset в Kafka
```

---

## 4. Обслуживание (retention)

| Что | Как часто | Запрос |
|-----|-----------|--------|
| `idempotency_keys` старше `expires_at` | каждые 10 мин | `idempotency.Store.DeleteExpired` батчами по 1000 |
| `outbox` опубликованные старше 7 дней | каждый час | `DELETE ... WHERE published_at < now() - interval '7 days'` батчами |
| `processed_events` старше 30 дней | раз в сутки | батчами |

**На будущее (раздел для README «что бы я сделал в проде»):**
- партиционирование `postings` по месяцу (`PARTITION BY RANGE (created_at)`);
- шардирование горячих системных счетов (`settlement.RUB#0..#N`);
- read-replica для выписок и reconciler'а;
- CDC через Debezium вместо polling-relay.
