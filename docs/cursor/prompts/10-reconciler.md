# Промпт 10: Reconciler

**Контекст:** @docs/adr/0007-reconciliation.md @docs/database.md @migrations/accounts @cmd/reconciler/main.go

---

## Сделай `internal/reconciler/`
- `type Check interface { Name() string; Run(ctx, q postgres.Querier) ([]Discrepancy, error) }`.
- Реализуй проверки 1–5 из ADR-0007 (6-ю — опционально через gRPC transfers). Каждая — один агрегирующий SQL, пример для G9:
  ```sql
  SELECT a.id, a.balance, COALESCE(SUM(p.amount), 0) AS postings_sum
  FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
  GROUP BY a.id HAVING a.balance <> COALESCE(SUM(p.amount), 0);
  ```
- `Run(ctx, pool)`: все проверки в одной `REPEATABLE READ READ ONLY` транзакции (консистентный снапшот!),
  запись результатов в `reconciliation_runs/discrepancies` отдельной транзакцией
  (reconciler-роль получает INSERT только на эти две таблицы — новая миграция с GRANT).
- `cmd/reconciler`: печатает отчёт (таблица в лог), push метрик в Prometheus Pushgateway опционально, exit 2 при расхождениях.

## Тесты (integration)
- Чистая БД после серии операций → 0 расхождений.
- Искусственно испорть данные (отключи триггер `ALTER TABLE postings DISABLE TRIGGER USER` в тесте, вставь «левую» проводку /
  поменяй balance) → каждая проверка находит свою аномалию.

Коммит: `feat(reconciler): ledger invariant checks`.
