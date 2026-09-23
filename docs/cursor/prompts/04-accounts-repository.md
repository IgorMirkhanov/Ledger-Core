# Промпт 04: Accounts repository (pgx)

**Контекст:** @docs/database.md @internal/accounts/service/ports.go @internal/accounts/domain @migrations/accounts/00001_init.sql @.cursor/rules/30-database.mdc

---

Задача: реализовать интерфейс `service.Repository` в `internal/accounts/repository/postgres.go`.

## Требования
- `type Repository struct{}`; `func New() *Repository`; `var _ service.Repository = (*Repository)(nil)`.
  Состояния нет: все методы принимают `postgres.Querier`.
- Используй ЭТАЛОННЫЕ запросы из `docs/database.md` §1.3 (блокировка с ORDER BY id, батч-update через unnest,
  вставка проводок одним INSERT ... SELECT unnest, keyset-выписка, SKIP LOCKED для холдов).
- Маппинг ошибок:
  - `pgx.ErrNoRows` → `domain.ErrAccountNotFound` / `domain.ErrHoldNotFound`;
  - `LockAccounts`: если вернулось меньше строк, чем уникальных id → `ErrAccountNotFound`;
  - unique `holds_reference_uniq` → верни sentinel `service.ErrHoldReferenceExists` (добавь его в service/ports.go);
  - unique `journal_entries_reference_uniq` → `service.ErrEntryReferenceExists`.
- Маппинг типов: `money.Currency` ↔ `CHAR(3)` через `money.ParseCurrency`; enum-ы Postgres ↔ string-типы домена;
  `uuid.Nil` ↔ NULL для `owner_id`, `code`, `journal_entry_id`.
- `InsertEntry`: вставить `journal_entries`, затем все postings одним запросом. Возвращать ошибку, если у posting не заполнен `BalanceAfter`
  (защита от забытого Apply: добавь в domain.Posting флаг или проверяй через отдельное поле `applied bool`, на твоё усмотрение, но с тестом).
- `GetSystemAccount(prefix, cur)` — по `code = prefix || '.' || cur`. Кэшировать id системных счетов в памяти
  (они неизменны) — через `sync.Map` внутри Repository допустимо.

## Тесты (integration, `tests/integration/accounts_repository_test.go`)
- CRUD счёта, List по owner (порядок по created_at).
- LockAccounts возвращает все счета; с несуществующим id → ErrAccountNotFound.
- **Порядок блокировок:** две горутины блокируют [A,B] и [B,A] 200 раз в своих tx с `pg_sleep(0.001)` между — нет дедлоков (`40P01`).
- InsertEntry + UpdateBalances → Statement возвращает строки в порядке DESC, пагинация по курсору без дублей/пропусков (вставь 25, читай по 10).
- LockExpiredHolds из двух tx параллельно не возвращает одинаковые холды.

Коммит: `feat(accounts): postgres repository`.
