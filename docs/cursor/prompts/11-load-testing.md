# Промпт 11: Нагрузочное тестирование (k6)

**Контекст:** @docs/api.md @scripts/demo.sh @deploy/grafana

---

## Сделай `tests/load/`
- `setup.js`: создаёт N=100 пользователей (dev token), по 2 счёта RUB, депозит 1 000 000.00 каждому.
- `transfers.js` сценарии:
  1. `steady`: constant-arrival-rate 300 rps переводов между случайными счетами, 3 мин.
  2. `hot_account`: 80% переводов на один счёт (демонстрация contention).
  3. `idempotent_retries`: каждый 5-й запрос повторяется с тем же ключом → проверка одинакового ответа.
  4. `read_mix`: 70% GET (счета, выписки), 30% POST.
- thresholds: `http_req_failed < 0.1%`, `p(99) < 300ms` для POST /transfers в steady.
- После прогона `make reconcile` должен дать 0 расхождений (добавь в `make load` последовательный вызов).
- `docs/benchmarks.md`: таблица результатов (железо, RPS, p50/p95/p99, ошибки), скриншоты Grafana, выводы
  (где узкое место: блокировки горячего счёта, пул соединений, relay).

Коммит: `test(load): k6 scenarios and benchmark report`.
