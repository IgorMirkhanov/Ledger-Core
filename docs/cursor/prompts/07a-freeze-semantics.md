# Промпт 07a: Единая семантика заморозки (исправление по ревью)

**Контекст:** @docs/adr/0009-account-freeze-semantics.md @internal/accounts/domain/account.go @internal/accounts/domain/journal.go @internal/accounts/service @tests/integration

---

## Проблема
Сейчас зачисление на `frozen` счёт через Deposit разрешено, а через CaptureHold запрещено отдельной проверкой
в use case. Одна операция «зачислить» ведёт себя по-разному в зависимости от канала. ADR-0009 фиксирует правило:
`frozen` запрещает только списание, `closed` запрещает всё. Правило проверяет только домен.

## Сделай
1. Удали из `CaptureHold` (и из любых других use case'ов accounts) собственные проверки статуса счёта.
   Статусы проверяют только `CanDebit` / `CanCredit` / `Reserve` / `JournalEntry.Apply`.
   Проверь grep'ом: `grep -rn "StatusFrozen\|StatusActive\|StatusClosed" internal/accounts/service internal/accounts/transport`
   должен быть пустым.
2. Тесты:
   - e2e (`tests/integration/e2e_transfers_test.go`): текущий сценарий «dest заморожен → failed» замени на
     «dest **закрыт** (`closed`) → compensating → failed/ACCOUNT_NOT_ACTIVE, source восстановлен».
   - Добавь: dest `frozen` → `completed`, баланс dest увеличен.
   - Добавь: source `frozen` → `failed/ACCOUNT_NOT_ACTIVE` на шаге hold, холдов нет, `transfer_steps` содержит hold/business_error.
   - Integration accounts: Deposit на `closed` → `ACCOUNT_NOT_ACTIVE`; Withdraw с `frozen` → `ACCOUNT_NOT_ACTIVE`.
   - Unit сервиса saga менять не нужно: фейковый клиент не знает о статусах.

## Критерии приёмки
`make test && make test-integration` зелёные.
Коммит: `fix(accounts): single freeze rule in domain (ADR-0009)`.
