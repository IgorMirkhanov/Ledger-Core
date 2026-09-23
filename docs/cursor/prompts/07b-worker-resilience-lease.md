# Промпт 07b: Устойчивость воркеров и аренда шагов саги (исправление по ревью)

**Контекст:** @docs/saga.md @docs/database.md (§2) @internal/transfers/service/saga.go @internal/transfers/repository/postgres.go @internal/transfers/worker/recovery.go @internal/accounts/worker @internal/platform/app/app.go

---

## Проблема 1: фоновый воркер роняет весь сервис
`Recovery.tick`, `HoldExpirer.drain` и `Janitor` возвращают ошибку БД из `Run`. `app.App` на ошибку компонента
отменяет всё и завершает процесс. Секундная недоступность Postgres (failover, рестарт, исчерпанный пул)
кладёт transfers или accounts целиком, вместе с gRPC API.

**Исправление.** Правило для всех воркеров: `Run` возвращает только `nil` при отмене ctx. Ошибка итерации →
`slog.Error` (с `component`, ошибкой и, где есть, id) → следующий тик по расписанию. Метрика
`worker_errors_total{worker}` (обычный Prometheus counter; если промпт 03 ещё не сделан, заведи его в пакете worker).
- HoldExpirer: если батч падает, повтори его по одному холду в отдельных транзакциях, чтобы один «ядовитый» холд
  (например, `Unreserve` > `held` из-за порчи данных) не блокировал истечение всех остальных. Ошибку отдельного
  холда логируй с `hold_id`.
- Relay (`internal/platform/outbox`) уже так себя ведёт, используй его как образец.

## Проблема 2: дублирующая работа саги
`ClaimPending` держит `SKIP LOCKED` только до COMMIT своей транзакции, поэтому:
- свежий перевод имеет `next_attempt_at = now`: пока `CreateTransfer` продвигает его в запросе, воркер через ≤1s
  забирает тот же перевод и шлёт в accounts тот же шаг;
- две реплики transfers после коммита захвата могут взять одни и те же id.
Корректность при этом не страдает (ключи идемпотентности + версии), но под нагрузкой, когда шаги медленные,
трафик в accounts удваивается ровно тогда, когда accounts и так перегружен.

**Исправление (аренда):**
1. `StepLease = 10 * time.Second` в `service` (должен быть больше таймаута вызова accounts, 3s, с запасом).
2. Repository:
   - `ClaimPending(ctx, q, now, lease time.Time, limit)`: запрос `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING id`
     из `docs/database.md` §2;
   - новый `Lease(ctx, q, id, until time.Time) error`: `UPDATE transfers SET next_attempt_at = $2 WHERE id = $1`
     **без** изменения `version` и `updated_at`.
3. `advanceOnce`, tx1: после `Lock` и проверки терминальности вызови `Lease(id, now + StepLease)`.
   `Update` в tx2 перезаписывает `next_attempt_at` значением из домена, так аренда снимается сама.
4. Обнови интерфейс `service.Repository`, фейки и docstring'и.

## Тесты
- Unit worker'ов (фейки): сервис возвращает ошибку на первой итерации, потом успех → `Run` не завершился,
  вторая итерация выполнена; `Run` возвращает `nil` после отмены ctx.
- HoldExpirer: батч из 3 холдов, один «ядовитый» → два других истекли.
- Integration repository: `ClaimPending` дважды подряд (две транзакции, одна за другой) → второй вызов не возвращает
  id из первого; после истечения аренды (передай `now` в будущем) id снова возвращаются. `Lease` не меняет `version`.
- Unit saga: фейковый AccountsClient блокирует `CreateHold` на канале; пока он заблокирован, вызови
  `ClaimPending(now)` → перевод не возвращается. После разблокировки перевод доходит до `completed`, `CreateHold` вызван ровно 1 раз.
- Существующие 10 сценариев саги и e2e должны остаться зелёными.

## Заодно
`.golangci.yml` теперь проверяет и файлы с тегом `integration`. Исправь 3 замечания в `tests/integration`
(goimports в `e2e_transfers_test.go`, prealloc в `accounts_grpc_test.go` и `transfers_repository_test.go`).

## Критерии приёмки
`make test && make test-integration && make lint` зелёные.
Коммиты: `fix(workers): never exit on iteration errors`, `fix(transfers): lease in-flight saga steps`.
