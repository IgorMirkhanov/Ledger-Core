# Промпт 01: Фундамент интеграционных тестов

**Режим Cursor:** Agent. **Контекст:** @docs/architecture.md @docs/database.md @internal/platform @migrations @.cursor/rules/40-testing.mdc

---

Задача: создать инфраструктуру интеграционных тестов и покрыть ими то, что уже реализовано в каркасе
(миграции, триггеры БД, idempotency store, outbox writer/relay). Бизнес-логику НЕ трогай.

## Сделай

1. `tests/integration/testenv/postgres.go` (build tag `integration`):
   - `func StartPostgres(t *testing.T) *pgxpool.Pool` на testcontainers-go (`postgres:17-alpine`), один контейнер на пакет
     (`TestMain` + `sync.Once`), на каждый тест — отдельная БД `CREATE DATABASE t_<random>` для изоляции.
   - `func MigratedPool(t *testing.T, fsys fs.FS) *pgxpool.Pool` применяет `postgres.Migrate`.
   - Очистка через `t.Cleanup`.
2. `tests/integration/testenv/kafka.go`: `StartRedpanda(t) (brokers []string)` (модуль testcontainers redpanda).
3. `tests/integration/accounts_schema_test.go`, тесты на уровне SQL:
   - сбалансированная запись вставляется;
   - несбалансированная падает на COMMIT с `check_violation`;
   - одна проводка на запись → ошибка;
   - валюта проводки ≠ валюта счёта → ошибка;
   - UPDATE/DELETE `postings` и `journal_entries` → ошибка;
   - `balance - held < 0` для customer → ошибка; для system (overdraft) → ок;
   - системные счета созданы для всех валют (5 валют × 3 = 15).
4. `tests/integration/idempotency_test.go`:
   - первый `Begin` → nil; `Complete`; повторный `Begin` → Record с ответом;
   - другой hash → `ErrKeyReused`;
   - rollback транзакции → ключ не сохранился, повтор выполняется;
   - **конкурентность**: 20 горутин с одним ключом, каждая в своей tx делает Begin → (если nil) insert в тестовую таблицу `side_effects` → Complete.
     Итог: ровно 1 строка в `side_effects`, остальные 19 получили Record.
5. `tests/integration/outbox_test.go`:
   - `Writer.Add` в транзакции + rollback → строки нет;
   - Relay с фейковым Publisher: публикует по порядку id, проставляет `published_at`;
   - Publisher возвращает ошибку → `published_at` NULL, `attempts` увеличен, `last_error` заполнен;
   - два Relay на одной БД → публикует только один (advisory lock), порядок не нарушен;
   - Relay с реальной Redpanda (`kafka.NewProducer`) → сообщение читается из топика, key = aggregate_id.
6. Добавь `github.com/stretchr/testify` и testcontainers в go.mod (`go mod tidy`).

## Критерии приёмки
- `make test-integration` зелёный локально (Docker запущен), `make lint` без замечаний.
- Тесты не используют `time.Sleep` для синхронизации (только `require.Eventually`).
- Коммит: `test(platform): integration test harness, schema/idempotency/outbox tests`.
