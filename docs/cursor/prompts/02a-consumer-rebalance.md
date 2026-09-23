# Промпт 02a: Consumer — безопасный rebalance (исправление по ревью)

**Контекст:** @internal/platform/kafka/consumer.go @tests/integration/kafka_consumer_test.go @tests/integration/idempotency_test.go

---

## Проблема
Consumer обрабатывает fetch параллельно по партициям и коммитит offset'ы вручную, но не блокирует rebalance.
Когда в группу входит вторая реплика (любой деплой или масштабирование), franz-go отзывает партиции
**во время** обработки. Дальше:
- `CommitRecords` для уже отозванной партиции падает (`REBALANCE_IN_PROGRESS` / `ILLEGAL_GENERATION`),
  `handlePartition` возвращает ошибку, `Run` завершается с ошибкой, и **сервис падает на каждом деплое**;
- либо коммит проходит для партиции, которой мы уже не владеем, и сдвигает offset нового владельца.

Документация franz-go (`BlockRebalanceOnPoll`): «By blocking rebalancing after you poll until you call
AllowRebalances, you can be sure that you commit records that your member currently owns».

## Исправление
1. В `NewConsumer` добавь `kgo.BlockRebalanceOnPoll()`.
2. В `Run` после `handleFetches` (на любом пути, включая ошибку и shutdown) вызывай `c.cl.AllowRebalance()`.
   Надёжнее всего через `defer` в отдельной функции `pollOnce(ctx) error`.
3. Ограничь объём обработки между poll'ами, чтобы не превысить rebalance timeout (по умолчанию 60s):
   замени `PollFetches` на `PollRecords(ctx, 500)`. Группировку по партициям сделай сам
   (`map[topicPartition][]*kgo.Record` с сохранением порядка) либо через `fetches.EachPartition` по результату `PollRecords`.
4. Ограничь суммарное время ретраев одного сообщения: `MaxAttempts × maxBackoff` должно укладываться примерно в 30s.
   Для дефолтов (5 × 2s) это уже так. Добавь проверку в `NewConsumer`: если сумма backoff превышает 30s,
   верни ошибку конфигурации.
5. Shutdown: handler сейчас получает ctx, который отменяется при остановке, поэтому in-flight операция в БД
   обрывается, а не «дообрабатывается». Передавай в handler `context.WithoutCancel(ctx)` с таймаутом 10s.
   Отмену `ctx` проверяй только между сообщениями и в backoff (как сейчас).

## Тесты (integration)
- `TestKafkaConsumer_RebalanceDuringProcessing`: топик с 4 партициями, 200 сообщений. Consumer A стартует,
  handler медленный (10ms). Через 300ms в ту же группу входит consumer B. Ожидается:
  ни один `Run` не вернул ошибку; каждое сообщение обработано минимум один раз (дубли допустимы: at-least-once);
  после остановки обоих новый consumer группы ничего не получает (все offset'ы закоммичены).
- `TestKafkaConsumer_ShutdownFinishesInFlight`: handler пишет в канал «начал», спит 300ms и пишет «закончил».
  Сразу после «начал» отменяем ctx. Ожидается: «закончил» пришёл, handler не увидел отменённый ctx,
  offset этого сообщения закоммичен.

## Заодно (мелкое)
- `TestIdempotency_ConcurrentSingleSideEffect`: сейчас победитель может закоммитить раньше, чем остальные дойдут
  до `Begin`, и тогда путь «ждём на уникальном индексе» не проверяется. В ветке победителя (`rec == nil`)
  перед `Complete` добавь `SELECT pg_sleep(0.2)`, чтобы остальные 19 гарантированно встали на блокировке.

## Критерии приёмки
`make test-integration` с `-race` зелёный, `make lint` без замечаний.
Коммит: `fix(platform): block rebalance during processing, finish in-flight record on shutdown`.
