# Промпт 11: Нагрузочное тестирование (k6)

**Контекст:** @docs/api.md @scripts/demo.sh @deploy/grafana

---

## Окружение для нагрузки
Gateway ограничивает запросы по IP (`RATE_LIMIT_IP_RPS`, по умолчанию 100/s) и по пользователю (`RATE_LIMIT_RPS`, 50/s).
k6 бьёт с одного адреса, поэтому с настройками по умолчанию тест измерит rate limiter, а не ledger.
- `docker-compose.load.yml` (override): `RATE_LIMIT_IP_RPS=100000`, `RATE_LIMIT_RPS=100000`, `LOG_LEVEL=warn`.
  `make load` поднимает окружение с этим override (`docker compose -f docker-compose.yml -f docker-compose.load.yml up -d`).
- Отдельный короткий сценарий `rate_limit` на **обычных** настройках: 1 пользователь, 200 rps 10 секунд →
  доля 429 больше 0, `Retry-After` присутствует. Это доказательство, что лимитер работает, а не отключён навсегда.
- В `docs/benchmarks.md` явно укажи, что замеры сделаны с отключёнными лимитами.

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
