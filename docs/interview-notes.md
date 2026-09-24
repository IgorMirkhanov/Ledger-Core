# Interview notes (Ledger Core)

Краткие ответы на типичные вопросы по проекту. Детали — в `docs/architecture.md`, ADR и `docs/saga.md`.

---

### 1. Почему не SERIALIZABLE для проводок по счетам?

SERIALIZABLE корректен «из коробки», но на горячих счетах даёт много откатов `40001` и непредсказуемый хвост латентности. Мы выбрали **READ COMMITTED + `SELECT … FOR UPDATE` в порядке `id`**: очередь на строке вместо каскада ретраев, дедлоки исключены единым порядком блокировок. Оптимистичная `version` остаётся для saga в `transfers`, где нельзя держать row-lock на время gRPC. Defense-in-depth: CHECK на доступный остаток и deferred-триггер двойной записи (G1). См. ADR-0003.

### 2. Что будет, если процесс упадёт между publish в Kafka и `UPDATE outbox SET published_at`?

Transactional outbox: событие уже в той же БД-транзакции, что и бизнес-изменение. Publish и mark published идут в одном проходе relay; если mark не закоммитился, строка остаётся с `published_at IS NULL` и будет опубликована снова (at-least-once). Потребитель дедуплицирует по `event_id` через inbox (`processed_events`). Фантомных событий без бизнес-факта нет: в Kafka нечего публиковать, пока outbox-строка не закоммичена вместе с проводкой.

### 3. Как масштабировать горячий счёт (например `settlement.RUB`)?

Сейчас все депозиты в валюте сериализуются на одной строке settlement — это сознательный bottleneck. Дальше: N суб-счетов `settlement.RUB#k`, выбор по `hash(key) % N`, reconciler суммирует их при проверке G2. Клиентские «звёзды» (много кредитов на один account) лечатся шардированием продукта/лимитами и асинхронным зачислением, а не снятием `FOR UPDATE`. Горизонтально масштабируем реплики сервисов; узкое место остаётся в строках Postgres.

### 4. Как работает компенсация в saga перевода?

Happy path: `created → HoldFunds → funds_held → CaptureHold → completed`. Бизнес-отказ на capture/hold → `compensating → ReleaseHold → failed`. Release идемпотентен: уже `released`/`expired` считается успехом компенсации. Оркестратор — `transfers`; деньги трогает только `accounts` через hold/capture/release. Recovery worker дожимает незавершённые статусы после крэша (G7). См. `docs/saga.md`.

### 5. Как обеспечивается идемпотентность API?

Таблица `idempotency_keys` в той же транзакции, что и операция: Begin по `(scope, key)` (+ hash тела), при конфликте — replay сохранённого ответа. Ключ несёт клиент (`Idempotency-Key`); gateway/сервисы добавляют namespace владельца, чтобы ключи разных пользователей не пересекались. Повтор с тем же ключом и телом не создаёт вторую проводку (G4). Для hold дополнительно уникальность business `reference`.

### 6. Зачем inbox у notifications?

Kafka даёт at-least-once. Inbox `processed_events` с PK по `event_id`: вставка и побочный эффект (запись уведомления) в одной транзакции. Повторная доставка того же события получает conflict на PK и пропускается — эффективно exactly-once на стороне потребителя (G6). Без inbox при ретрае consumer можно было бы слать дубли пользователю.

### 7. Почему rate limit по IP стоит до Auth?

Проверка JWT дороже, чем Redis INCR. Если лимит только после Auth, поток мусорных токенов получает 401 без ограничения и грузит CPU. Два слоя: `RateLimitIP` до Auth (`rl:ip:<ip>`), `RateLimitUser` после (`rl:user:<id>`). Оба fail-open с метрикой при недоступности Redis. Нагрузочные прогоны поднимают лимиты через `docker-compose.load.yml`.

### 8. Зачем trusted proxies для `X-Forwarded-For`?

Без фильтра клиент подставляет произвольный XFF и обходит IP-лимит. `TRUSTED_PROXY_CIDRS` пуст → берём только `RemoteAddr`, заголовки игнорируются. Если peer в доверенной сети — идём по XFF справа налево и берём первый адрес вне доверенных CIDR (левые значения контролирует клиент). Так лимит бьёт по реальному клиенту за ingress.

### 9. В чём разница гарантий G1, G2 и G9?

**G1** — каждая journal entry сбалансирована по валютам (сумма postings = 0); домен + deferred trigger. **G2** — сумма балансов всех счетов по валюте = 0 (следствие G1 + системные `settlement.*`); ловит reconciler. **G9** — `balance` на счёте равен сумме его postings (кэш обновляется в той же TX); reconciler сверяет кэш с журналом. Деньги не создаются и не теряются, даже если кэш или межсервисное состояние временно расходятся до сверки.

### 10. Что такое lease pattern у recovery worker?

Несколько реплик `transfers` захватывают работу так:
`UPDATE … SET next_attempt_at = now()+lease WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)`.
`SKIP LOCKED` защищает только до COMMIT; без аренды после коммита другую реплику (или API Advance) сразу увидит тот же перевод. Аренда отодвигает `next_attempt_at` без смены `version`/статуса saga. Упал воркер — аренда истекает, перевод подбирает другой. См. `docs/database.md`.

### 11. Почему database per service, а reconciler читает чужую БД?

Сервисы не шарят схему и не ходят SQL крест-накрест — эволюция и владение данными изолированы. Исключение: read-only роль reconciler на `accounts` (ADR-0007), чтобы проверить G1/G2/G9 без дублирования всего журнала. Переводы сверяются по gRPC/снимкам статусов, не JOIN-ом двух OLTP-БД из онлайна.

### 12. Чем outbox лучше «записали в БД, потом в Kafka»?

Классический dual-write теряет события при крэше между COMMIT и produce или публикует событие без факта при обратном порядке. Outbox атомарно с бизнес-TX; relay с `acks=all` и advisory lock на одного лидера сохраняет порядок. Цена — лаг до `OUTBOX_POLL_INTERVAL` и метрики pending/lag. CDC/Debezium — следующий шаг, не требование MVP (ADR-0005).

### 13. Как перевод гарантированно становится терминальным (G7)?

Статусы только через Advance; TTL холдов; recovery worker с lease крутит `created` / `funds_held` / `compensating`. Бизнес-отказы ведут в `failed` через compensate; технические ошибки ретраятся с backoff. Клиент может получить 202 и дождаться GET; зависший `funds_held` не остаётся навсегда — expire/release вернёт деньги на source.

### 14. Почему холды, а не сразу debit/credit в оркестраторе?

Hold резервирует `held` без финальной проводки на dest; capture атомарно списывает source и кредитует dest; release откатывает резерв без «мусорных» сторно в журнале. Так компенсация — это release, а не reverse-posting. Идемпотентные reference на hold защищают от двойного резерва при ретраях saga-шага.

### 15. Что проверяет chaos / load, чего не видят unit-тесты?

Unit/integration ловят алгоритмы на чистой БД. k6 + chaos бьют по реальному compose: kill accounts/transfers mid-flight, Kafka down с ростом outbox, рестарт Postgres с 503→200 на readyz. Критерий PASS — не «ноль HTTP ошибок в окне отказа», а **терминальные переводы + reconcile без расхождений**. Это проверка G5–G7 и reconnect пулов под нагрузкой ноутбука/CI.
