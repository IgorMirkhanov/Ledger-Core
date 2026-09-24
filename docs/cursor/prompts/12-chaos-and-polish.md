# Промпт 12: Chaos-сценарии и финальная полировка

**Контекст:** @docs @README.md @docker-compose.yml

---

## Chaos (`tests/chaos/`, bash + docker compose)
Все сценарии запускаются с `docker-compose.load.yml` из промпта 11 (иначе k6 упрётся в rate limit).
1. `kill_accounts_mid_load.sh`: k6 steady в фоне → `docker compose kill accounts` на 10s → `start` →
   дождаться, пока все переводы станут терминальными (GET по id) → `make reconcile` = 0.
2. `kill_transfers.sh`: то же для transfers (recovery worker после рестарта доводит переводы).
3. `kafka_down.sh`: остановить redpanda на 30s → переводы продолжают работать, `outbox_pending_events` растёт,
   после старта падает до 0, notifications получают все события без дублей.
4. `postgres_restart.sh`: рестарт PG → сервисы переподключаются (readyz 503 → 200), данные консистентны.
Каждый скрипт печатает PASS/FAIL. Результаты: раздел в `docs/benchmarks.md`.

## Полировка
- README: GIF/asciinema демо (`scripts/demo.sh`), бейджи CI, актуальные цифры из benchmarks.
- `golangci-lint` + `govulncheck` в CI. Покрытие `internal/...` ≥ 70% (отчёт в CI summary).
- Проверь все TODO: `grep -rn "TODO(prompt" .` — должно быть пусто.
- `docs/interview-notes.md`: 15 вопросов, которые зададут по проекту, и краткие ответы
  (почему не SERIALIZABLE, что будет при падении между publish и UPDATE outbox, как масштабировать горячий счёт и т.д.).

Коммит: `test(chaos): failure scenarios`, `docs: final polish`.
