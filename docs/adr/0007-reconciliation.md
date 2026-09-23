# ADR-0007: Сверка как отдельный job с read-only доступом

**Статус:** принято

## Решение
`reconciler` запускается по расписанию, подключается к БД `accounts` под read-only ролью
(осознанное исключение из database-per-service: в банке сверка — отдельная контрольная функция),
к transfers ходит по gRPC.

Проверки:
1. G1: каждая `journal_entry` сбалансирована по валютам.
2. G2: сумма балансов всех счетов по валюте = 0.
3. G9: `accounts.balance = SUM(postings.amount)` и `= balance_after` последней проводки.
4. G3: `accounts.held = SUM(holds.amount WHERE status='active')`.
5. Активных холдов с `expires_at < now() - 5m` нет (expirer работает).
6. Перевод `completed` ⇔ существует `journal_entry(reference_id=transfer_id, kind=transfer)` (выборочно за последние N часов).

Результат: `reconciliation_runs`, `reconciliation_discrepancies`, метрика, exit code 2 при расхождениях.

## Последствия
+ Независимый контроль: даже баг в коде сервиса будет обнаружен.
− Полный пересчёт дорог на больших объёмах → в проде инкрементально по дням/партициям.
