# Промпт 02: Kafka consumer с inbox, ретраями и DLQ

**Контекст:** @docs/events.md @internal/platform/kafka @internal/platform/outbox/outbox.go @migrations/notifications

---

Задача: переиспользуемый consumer в `internal/platform/kafka/consumer.go` (franz-go consumer group).

## API
```go
type Handler func(ctx context.Context, msg Message) error   // Message: Topic, Partition, Offset, Key, Value, Headers, Envelope (распарсенный outbox.Envelope)

type ConsumerConfig struct {
    Brokers []string; Group string; Topics []string
    MaxAttempts int            // по умолчанию 5
    DLQTopic string            // "ledger.notifications.dlq"
    RetryBackoff func(attempt int) time.Duration
}

func NewConsumer(cfg ConsumerConfig, h Handler, log *slog.Logger) (*Consumer, error)
func (c *Consumer) Name() string
func (c *Consumer) Run(ctx context.Context) error   // app.Runner
```

## Поведение
- `kgo.DisableAutoCommit()`. Обработка по партициям последовательно (порядок внутри ключа), разные партиции параллельно
  (goroutine per partition, см. пример franz-go `goroutine_per_partition_consuming`).
- Offset коммитится ТОЛЬКО после успешного `Handler` (или после отправки в DLQ).
- Ошибка Handler: до `MaxAttempts` ретраев с backoff **в памяти** (не блокирует другие партиции), затем сообщение
  публикуется в DLQ с заголовками `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-error`, `x-attempts`.
- `ErrPermanent` (экспортируй sentinel) → сразу в DLQ, без ретраев (например, невалидный JSON).
- Невалидный envelope → DLQ.
- Graceful shutdown: при отмене ctx дообработать текущее сообщение, закоммитить offsets, закрыть клиент.
- Извлекать `traceparent` из заголовков в ctx (пока через TODO(prompt-03), если otel ещё нет).

## Тесты (integration, Redpanda из testenv)
- 10 сообщений → handler вызван 10 раз, offsets закоммичены (новый consumer той же группы ничего не получает).
- Handler падает 2 раза, потом ok → обработано 1 раз успешно, в DLQ пусто.
- Handler всегда падает → сообщение в DLQ с нужными заголовками, offset закоммичен.
- Порядок: 100 сообщений с одним ключом обрабатываются строго по порядку.

Коммит: `feat(platform): kafka consumer with retries and DLQ`.
