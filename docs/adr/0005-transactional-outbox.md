# ADR-0005: Transactional Outbox + polling relay

**Статус:** принято

## Контекст
Нельзя атомарно записать в Postgres и Kafka (dual write problem).

## Решение
- Событие пишется в таблицу `outbox` в той же транзакции.
- Relay в каждом сервисе: `SELECT ... FOR UPDATE SKIP LOCKED` батчами → `ProduceSync(acks=all)` → `published_at`.
- Один активный relay на сервис: `pg_try_advisory_lock` (сохраняет порядок событий).
- At-least-once; потребители дедуплицируют по `event_id` (inbox).

## Альтернативы
- Debezium CDC (логическая репликация): меньше латентность и нагрузка на БД, но ещё одна
  тяжёлая компонента в инфраструктуре. Указано как следующий шаг.
- Публикация в Kafka после COMMIT без outbox: теряет события при падении процесса.

## Последствия
+ Нет потерянных и «фантомных» событий.
− Задержка публикации до `OUTBOX_POLL_INTERVAL` (200ms). Метрика `outbox_publish_lag_seconds`.
