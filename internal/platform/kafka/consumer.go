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

	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
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
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	return &Consumer{cl: cl, cfg: cfg, h: h, log: log.With(slog.String("component", "kafka-consumer"))}, nil
}

func (c *Consumer) Name() string { return "kafka-consumer" }

// Run polls until ctx is cancelled. The in-flight record is finished and its
// offset committed before Run returns. Records are valid only until the next
// poll, so each fetch is fully processed first; partitions of one fetch run
// in parallel, so a retry does not block other partitions of that fetch.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := c.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return nil
		}
		for _, ferr := range fetches.Errors() {
			c.log.Warn("fetch error", slog.String("topic", ferr.Topic), slog.Any("error", ferr.Err))
		}
		if err := c.handleFetches(ctx, fetches); err != nil {
			if errors.Is(err, errShutdown) {
				return nil
			}
			return err
		}
	}
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
	for _, rec := range recs {
		if err := c.processRecord(ctx, rec); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return errShutdown
		}
	}
	return nil
}

func (c *Consumer) processRecord(ctx context.Context, rec *kgo.Record) error {
	// The in-flight record must complete across shutdown so its offset can be committed.
	work := context.WithoutCancel(ctx)
	msg := Message{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   headerMap(rec),
	}
	// TODO(prompt-03): extract W3C traceparent from headers into work via the otel propagator.
	if err := json.Unmarshal(rec.Value, &msg.Envelope); err != nil {
		return c.toDLQ(work, rec, fmt.Errorf("invalid envelope: %w", err), 0)
	}
	var last error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		err := c.h(work, msg)
		if err == nil {
			return c.commit(work, rec)
		}
		last = err
		if errors.Is(err, ErrPermanent) {
			return c.toDLQ(work, rec, err, attempt)
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
		// Finish this record even while the process is shutting down.
		pause(c.cfg.RetryBackoff(attempt))
	}
	return c.toDLQ(work, rec, last, c.cfg.MaxAttempts)
}

func (c *Consumer) toDLQ(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
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
	if err := c.commit(ctx, rec); err != nil {
		return err
	}
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

func pause(d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	<-timer.C
}
