package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
)

const (
	// maxRecordsPerPoll bounds work between AllowRebalance calls so a poll
	// stays inside the broker rebalance timeout.
	maxRecordsPerPoll = 500
	// maxRetryBudget is the longest in-memory retry pause for one record.
	maxRetryBudget = 30 * time.Second
	// handlerTimeout lets the in-flight handler finish after Run is cancelled.
	handlerTimeout = 10 * time.Second
)

// errShutdown stops the partition loop after the in-flight record. Run maps it to a nil exit.
var errShutdown = errors.New("shutdown")

// ErrPermanent tells the consumer to publish the record to the DLQ without retrying.
var ErrPermanent = errors.New("permanent")

// Handler processes one Kafka record. A nil error commits the offset.
// A non-nil error is retried up to MaxAttempts, then the record goes to the DLQ.
type Handler func(ctx context.Context, msg Message) error

// Message is one consumed record with its parsed outbox envelope.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Envelope  outbox.Envelope
}

// ConsumerConfig configures a consumer group.
type ConsumerConfig struct {
	Brokers      []string
	Group        string
	Topics       []string
	MaxAttempts  int // default 5
	DLQTopic     string
	RetryBackoff func(attempt int) time.Duration
}

// Consumer is a consumer group that commits offsets only after a successful
// handler call or a DLQ publish. It implements app.Runner.
type Consumer struct {
	cl  *kgo.Client
	cfg ConsumerConfig
	h   Handler
	log *slog.Logger
}

// NewConsumer starts no network I/O beyond client construction.
func NewConsumer(cfg ConsumerConfig, h Handler, log *slog.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("kafka: consumer: brokers, group and topics are required")
	}
	if h == nil {
		return nil, errors.New("kafka: consumer: nil handler")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "ledger.notifications.dlq"
	}
	if cfg.RetryBackoff == nil {
		cfg.RetryBackoff = defaultBackoff
	}
	if err := validateRetryBudget(cfg.MaxAttempts, cfg.RetryBackoff); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	return &Consumer{cl: cl, cfg: cfg, h: h, log: log.With(slog.String("component", "kafka-consumer"))}, nil
}

func (c *Consumer) Name() string { return "kafka-consumer" }

// Run polls until ctx is cancelled. A finished prefix of a partition is committed
// at its last processed record; the rest of the fetch stays uncommitted.
// Partitions of one fetch run in parallel. Rebalance is blocked from poll until
// the fetch is finished, so a commit cannot land on a partition this member no
// longer owns. Retry backoff stops when ctx is cancelled. The handler of the
// in-flight record keeps running after cancel, bounded by handlerTimeout.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.pollOnce(ctx); err != nil {
			if errors.Is(err, errShutdown) {
				return nil
			}
			return err
		}
	}
}

// pollOnce fetches one bounded batch and processes it. AllowRebalance runs on
// every exit, including fetch errors and shutdown, and before Run closes the
// client. Close waits for a rebalance; skipping AllowRebalance deadlocks it.
func (c *Consumer) pollOnce(ctx context.Context) error {
	fetches := c.cl.PollRecords(ctx, maxRecordsPerPoll)
	defer c.cl.AllowRebalance()
	if fetches.IsClientClosed() {
		return nil
	}
	for _, ferr := range fetches.Errors() {
		if errors.Is(ferr.Err, context.Canceled) || errors.Is(ferr.Err, kgo.ErrClientClosed) {
			continue
		}
		c.log.Warn("fetch error", slog.String("topic", ferr.Topic), slog.Any("error", ferr.Err))
	}
	if err := c.handleFetches(ctx, fetches); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return errShutdown
	}
	return nil
}

func (c *Consumer) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if p.Err != nil || len(p.Records) == 0 {
			return
		}
		recs := make([]*kgo.Record, len(p.Records))
		copy(recs, p.Records)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.handlePartition(ctx, recs); err != nil {
				mu.Lock()
				if first == nil || (errors.Is(first, errShutdown) && !errors.Is(err, errShutdown)) {
					first = err
				}
				mu.Unlock()
			}
		}()
	})
	wg.Wait()
	return first
}

func (c *Consumer) handlePartition(ctx context.Context, recs []*kgo.Record) error {
	// Commit only the last record this partition actually finished. Committing the
	// whole fetch would advance the offset past records that were never handled.
	var last *kgo.Record
	var procErr error
	for _, rec := range recs {
		if ctx.Err() != nil {
			procErr = errShutdown
			break
		}
		done, err := c.processRecord(ctx, rec)
		if done {
			last = rec
		}
		if err != nil {
			procErr = err
			break
		}
	}
	if last != nil {
		// Shutdown must still commit the prefix that was finished.
		if err := c.commit(context.WithoutCancel(ctx), last); err != nil {
			return err
		}
	}
	return procErr
}

func (c *Consumer) processRecord(ctx context.Context, rec *kgo.Record) (bool, error) {
	ctx, span := startConsumerSpan(ctx, rec)
	defer span.End()
	done, err := c.processTraced(ctx, rec)
	if err != nil && !errors.Is(err, errShutdown) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return done, err
}

// startConsumerSpan continues the producer's trace (W3C traceparent in the record headers)
// with a CONSUMER span, so one transfer is visible from the HTTP request to the notification.
func startConsumerSpan(ctx context.Context, rec *kgo.Record) (context.Context, trace.Span) {
	carrier := observability.MapCarrier{}
	for _, h := range rec.Headers {
		carrier[h.Key] = string(h.Value)
	}
	ctx = observability.ExtractTraceparent(ctx, carrier)
	return otel.Tracer("ledger-core/kafka").Start(ctx, "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
}

func (c *Consumer) processTraced(ctx context.Context, rec *kgo.Record) (bool, error) {
	msg := Message{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   headerMap(rec),
	}
	if err := json.Unmarshal(rec.Value, &msg.Envelope); err != nil {
		if err := c.publishDLQ(ctx, rec, fmt.Errorf("invalid envelope: %w", err), 0); err != nil {
			return false, err
		}
		return true, nil
	}
	var last error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return false, errShutdown
		}
		err := c.invoke(ctx, msg)
		if err == nil {
			observability.KafkaConsumerMessages.WithLabelValues(rec.Topic, "ok").Inc()
			return true, nil
		}
		if ctx.Err() != nil {
			return false, errShutdown
		}
		last = err
		if errors.Is(err, ErrPermanent) {
			if err := c.publishDLQ(ctx, rec, err, attempt); err != nil {
				return false, err
			}
			return true, nil
		}
		if attempt == c.cfg.MaxAttempts {
			break
		}
		c.log.Warn("handler failed, will retry",
			slog.String("topic", rec.Topic),
			slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)
		observability.KafkaConsumerMessages.WithLabelValues(rec.Topic, "retry").Inc()
		if err := pause(ctx, c.cfg.RetryBackoff(attempt)); err != nil {
			return false, errShutdown
		}
	}
	if ctx.Err() != nil {
		return false, errShutdown
	}
	if err := c.publishDLQ(ctx, rec, last, c.cfg.MaxAttempts); err != nil {
		return false, err
	}
	return true, nil
}

// invoke gives the handler a context that survives Run cancellation, so an
// in-flight database write can commit. The parent ctx is what backoff and the
// gap between records observe.
func (c *Consumer) invoke(ctx context.Context, msg Message) error {
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handlerTimeout)
	defer cancel()
	return c.h(hctx, msg)
}

func (c *Consumer) publishDLQ(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	out := &kgo.Record{
		Topic: c.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: "x-original-topic", Value: []byte(rec.Topic)},
			{Key: "x-original-partition", Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: "x-original-offset", Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: "x-error", Value: []byte(msg)},
			{Key: "x-attempts", Value: []byte(strconv.Itoa(attempts))},
		},
	}
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.cl.ProduceSync(pubCtx, out).FirstErr(); err != nil {
		return fmt.Errorf("kafka: dlq publish: %w", err)
	}
	observability.KafkaConsumerMessages.WithLabelValues(rec.Topic, "dlq").Inc()
	c.log.Warn("record sent to dlq",
		slog.String("topic", rec.Topic),
		slog.Int64("offset", rec.Offset),
		slog.Int("attempts", attempts),
		slog.String("error", msg),
	)
	return nil
}

func (c *Consumer) commit(ctx context.Context, rec *kgo.Record) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(cctx, rec); err != nil {
		return fmt.Errorf("kafka: commit: %w", err)
	}
	return nil
}

func headerMap(rec *kgo.Record) map[string]string {
	if len(rec.Headers) == 0 {
		return map[string]string{}
	}
	h := make(map[string]string, len(rec.Headers))
	for _, hdr := range rec.Headers {
		h[hdr.Key] = string(hdr.Value)
	}
	return h
}

func validateRetryBudget(maxAttempts int, backoff func(int) time.Duration) error {
	var sum time.Duration
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		d := backoff(attempt)
		if d < 0 || d > maxRetryBudget || sum > maxRetryBudget-d {
			return fmt.Errorf("kafka: consumer: retry budget exceeds %s", maxRetryBudget)
		}
		sum += d
	}
	return nil
}

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	d := 50 * time.Millisecond << shift
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func pause(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
