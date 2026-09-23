# Промпт 09: Notifications service

**Контекст:** @docs/events.md @migrations/notifications @internal/platform/kafka (consumer из промпта 02) @cmd/notifications/main.go

---

## Сделай `internal/notifications/`
- `Handler` для consumer'а (топики `ledger.accounts.v1`, `ledger.transfers.v1`):
  одна транзакция: `INSERT processed_events ... ON CONFLICT DO NOTHING` → если 0 строк, return nil (дубль) →
  по таблице маппинга (`docs/events.md`, «Уведомления») создать `notifications` (status=pending) → COMMIT.
  Неизвестный event_type → записать в processed_events и пропустить (forward compatibility).
- `Sender` интерфейс + `LogSender` (пишет в лог «sent email to user ...»). `Dispatcher` (app.Runner):
  раз в 1s берёт `pending` (`FOR UPDATE SKIP LOCKED LIMIT 50`), отправляет, `sent`/`attempts++`, после 5 неудач `failed`.
- Шаблоны сообщений: `text/template`, embed.
- Wiring в main (закрой TODO), readiness: postgres + kafka.

## Тесты
- Unit: маппинг событие → уведомление (table-driven по всем event_type).
- Integration: одно событие, доставленное 3 раза → одно уведомление. Невалидный JSON → DLQ.

Коммит: `feat(notifications): inbox consumer and dispatcher`.
