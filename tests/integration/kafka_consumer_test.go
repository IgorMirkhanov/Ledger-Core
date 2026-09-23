//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestKafkaConsumer_CommitsOffsets(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	ensureTopic(t, brokers, topic)
	produceEnvelopes(t, brokers, topic, 10, func(i int) string { return fmt.Sprintf("k-%d", i) })

	adm := adminClient(t, brokers)
	var got atomic.Int32
	stop := runConsumer(t, brokers, topic, topic+".dlq", 5, func(_ context.Context, _ kafka.Message) error {
		got.Add(1)
		return nil
	})
	require.Eventually(t, func() bool { return got.Load() == 10 }, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, int64(10), committedOffset(adm, topicName(t), topic))
	stop()

	var again atomic.Int32
	runConsumer(t, brokers, topic, topic+".dlq", 5, func(_ context.Context, _ kafka.Message) error {
		again.Add(1)
		return nil
	})
	require.Never(t, func() bool { return again.Load() > 0 }, 3*time.Second, 100*time.Millisecond)
}

func TestKafkaConsumer_CommitsProcessedPrefixOnly(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	ensureTopic(t, brokers, topic)
	produceEnvelopes(t, brokers, topic, 10, func(i int) string { return fmt.Sprintf("k-%d", i) })

	adm := adminClient(t, brokers)
	blocked := make(chan struct{})
	var once sync.Once
	var got atomic.Int32
	stop := runConsumer(t, brokers, topic, topic+".dlq", 5, func(ctx context.Context, _ kafka.Message) error {
		if got.Add(1) <= 5 {
			return nil
		}
		once.Do(func() { close(blocked) })
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-blocked:
	case <-time.After(20 * time.Second):
		t.Fatal("handler did not reach the unprocessed tail")
	}
	stop()

	require.Eventually(t, func() bool {
		return committedOffset(adm, topicName(t), topic) == 5
	}, 10*time.Second, 50*time.Millisecond)

	var tail atomic.Int32
	runConsumer(t, brokers, topic, topic+".dlq", 5, func(context.Context, kafka.Message) error {
		tail.Add(1)
		return nil
	})
	require.Eventually(t, func() bool { return tail.Load() == 5 }, 20*time.Second, 50*time.Millisecond)
}

func TestKafkaConsumer_RetryStopsOnCancel(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)
	produceEnvelopes(t, brokers, topic, 1, func(int) string { return "k" })

	var calls atomic.Int32
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       topicName(t),
		Topics:      []string{topic},
		MaxAttempts: 5,
		DLQTopic:    dlq,
		// One pause is longer than the shutdown deadline. Five pauses of 6s
		// stay inside the consumer's 30s retry budget.
		RetryBackoff: func(int) time.Duration {
			return 6 * time.Second
		},
	}, func(context.Context, kafka.Message) error {
		calls.Add(1)
		return errors.New("temporary")
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool { return calls.Load() == 1 }, 20*time.Second, 20*time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown waited out the retry backoff")
	}
	require.Less(t, time.Since(started), 5*time.Second)
	require.Equal(t, int64(0), logEndOffset(adminClient(t, brokers), dlq))
	require.Equal(t, int32(1), calls.Load())
}

func TestKafkaConsumer_RebalanceDuringProcessing(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	ensureTopicPartitions(t, brokers, topic, 4)
	const n = 200
	produceEnvelopes(t, brokers, topic, n, func(i int) string { return fmt.Sprintf("k-%d", i) })

	var mu sync.Mutex
	seen := make(map[int]int, n)
	handler := func(_ context.Context, msg kafka.Message) error {
		time.Sleep(10 * time.Millisecond)
		var body struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(msg.Envelope.Data, &body); err != nil {
			return fmt.Errorf("%w: %w", kafka.ErrPermanent, err)
		}
		mu.Lock()
		seen[body.N]++
		mu.Unlock()
		return nil
	}

	group := topicName(t)
	stopA := startMember(t, brokers, topic, group, handler)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) > 0
	}, 20*time.Second, 20*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	stopB := startMember(t, brokers, topic, group, handler)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	}, 30*time.Second, 20*time.Millisecond)
	require.NoError(t, stopA())
	require.NoError(t, stopB())

	var again atomic.Int32
	runConsumer(t, brokers, topic, topic+".dlq", 5, func(context.Context, kafka.Message) error {
		again.Add(1)
		return nil
	})
	require.Never(t, func() bool { return again.Load() > 0 }, 3*time.Second, 100*time.Millisecond)
}

func TestKafkaConsumer_ShutdownFinishesInFlight(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	ensureTopic(t, brokers, topic)
	produceEnvelopes(t, brokers, topic, 1, func(int) string { return "k" })

	started := make(chan struct{})
	finished := make(chan struct{})
	var sawCancel atomic.Bool
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:      brokers,
		Group:        topicName(t),
		Topics:       []string{topic},
		DLQTopic:     topic + ".dlq",
		RetryBackoff: func(int) time.Duration { return 0 },
	}, func(ctx context.Context, _ kafka.Message) error {
		if ctx.Err() != nil {
			sawCancel.Store(true)
		}
		close(started)
		time.Sleep(300 * time.Millisecond)
		if ctx.Err() != nil {
			sawCancel.Store(true)
		}
		close(finished)
		return nil
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		done <- c.Run(ctx)
		close(exited)
	}()
	t.Cleanup(func() {
		cancel()
		<-exited
	})

	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight handler did not finish after cancel")
	}
	require.False(t, sawCancel.Load())

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not exit after the in-flight record")
	}
	require.Equal(t, int64(1), committedOffset(adminClient(t, brokers), topicName(t), topic))
}

func TestKafkaConsumer_RetriesThenSucceeds(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)
	produceEnvelopes(t, brokers, topic, 1, func(int) string { return "k" })

	adm := adminClient(t, brokers)
	var calls atomic.Int32
	runConsumer(t, brokers, topic, dlq, 5, func(_ context.Context, _ kafka.Message) error {
		if calls.Add(1) <= 2 {
			return errors.New("temporary")
		}
		return nil
	})
	require.Eventually(t, func() bool { return calls.Load() == 3 }, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, int64(1), committedOffset(adm, topicName(t), topic))
	require.Equal(t, int64(0), logEndOffset(adm, dlq))
}

func TestKafkaConsumer_ExhaustedAttemptsGoToDLQ(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)
	produceEnvelopes(t, brokers, topic, 1, func(int) string { return "k" })

	adm := adminClient(t, brokers)
	runConsumer(t, brokers, topic, dlq, 3, func(_ context.Context, _ kafka.Message) error {
		return errors.New("always")
	})
	require.Eventually(t, func() bool { return logEndOffset(adm, dlq) == 1 }, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, int64(1), committedOffset(adm, topicName(t), topic))

	rec := consumeOne(t, brokers, dlq)
	h := recordHeaders(rec)
	require.Equal(t, topic, h["x-original-topic"])
	require.Equal(t, "0", h["x-original-partition"])
	require.Equal(t, "0", h["x-original-offset"])
	require.Equal(t, "3", h["x-attempts"])
	require.Contains(t, h["x-error"], "always")
}

func TestKafkaConsumer_SameKeyIsOrdered(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	ensureTopic(t, brokers, topic)
	const n = 100
	produceEnvelopes(t, brokers, topic, n, func(int) string { return "same" })

	var (
		mu   sync.Mutex
		seen []int
	)
	runConsumer(t, brokers, topic, topic+".dlq", 5, func(_ context.Context, msg kafka.Message) error {
		var body struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(msg.Envelope.Data, &body); err != nil {
			return fmt.Errorf("%w: %w", kafka.ErrPermanent, err)
		}
		mu.Lock()
		seen = append(seen, body.N)
		mu.Unlock()
		return nil
	})
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	}, 30*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, n)
	for i := range seen {
		require.Equal(t, i, seen[i])
	}
}

func TestKafkaConsumer_InvalidEnvelopeGoesToDLQ(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic, Key: []byte("bad"), Value: []byte("not-json"),
	}).FirstErr())

	adm := adminClient(t, brokers)
	var calls atomic.Int32
	runConsumer(t, brokers, topic, dlq, 5, func(context.Context, kafka.Message) error {
		calls.Add(1)
		return nil
	})
	require.Eventually(t, func() bool { return logEndOffset(adm, dlq) == 1 }, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, int32(0), calls.Load())
	require.Contains(t, recordHeaders(consumeOne(t, brokers, dlq))["x-error"], "invalid envelope")
}

func TestKafkaConsumer_PermanentErrorSkipsRetries(t *testing.T) {
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)
	produceEnvelopes(t, brokers, topic, 1, func(int) string { return "k" })

	adm := adminClient(t, brokers)
	var calls atomic.Int32
	runConsumer(t, brokers, topic, dlq, 5, func(context.Context, kafka.Message) error {
		calls.Add(1)
		return kafka.ErrPermanent
	})
	require.Eventually(t, func() bool {
		return calls.Load() == 1 && logEndOffset(adm, dlq) == 1
	}, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, "1", recordHeaders(consumeOne(t, brokers, dlq))["x-attempts"])
}

func startMember(t *testing.T, brokers []string, topic, group string, h kafka.Handler) func() error {
	t.Helper()
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:      brokers,
		Group:        group,
		Topics:       []string{topic},
		DLQTopic:     topic + ".dlq",
		RetryBackoff: func(int) time.Duration { return 0 },
	}, h, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop := func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func ensureTopicPartitions(t *testing.T, brokers []string, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).CreateTopics(ctx, partitions, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, resp.Error())
}

func runConsumer(t *testing.T, brokers []string, topic, dlq string, attempts int, h kafka.Handler) func() {
	t.Helper()
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:      brokers,
		Group:        topicName(t),
		Topics:       []string{topic},
		MaxAttempts:  attempts,
		DLQTopic:     dlq,
		RetryBackoff: func(int) time.Duration { return 0 },
	}, h, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

func topicName(t *testing.T) string {
	t.Helper()
	return "it." + strings.ReplaceAll(t.Name(), "/", ".")
}

func produceEnvelopes(t *testing.T, brokers []string, topic string, n int, keyFn func(i int) string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	recs := make([]*kgo.Record, n)
	for i := range n {
		key := keyFn(i)
		env := outbox.Envelope{
			EventID:       uuid.Must(uuid.NewV7()),
			EventType:     "account.credited",
			SchemaVersion: 1,
			AggregateType: "account",
			AggregateID:   key,
			OccurredAt:    time.Now().UTC(),
			Producer:      "it",
			Data:          json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		}
		body, err := json.Marshal(env)
		require.NoError(t, err)
		recs[i] = &kgo.Record{Topic: topic, Key: []byte(key), Value: body}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, recs...).FirstErr())
}

func logEndOffset(adm *kadm.Client, topic string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	listed, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return -1
	}
	off, ok := listed.Lookup(topic, 0)
	if !ok || off.Err != nil {
		return -1
	}
	return off.Offset
}

func committedOffset(adm *kadm.Client, group, topic string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return -1
	}
	off, ok := resp.Lookup(topic, 0)
	if !ok || off.Err != nil {
		return -1
	}
	return off.At
}

func consumeOne(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	var got *kgo.Record
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if got == nil {
				got = r
			}
		})
		return got != nil
	}, 15*time.Second, 50*time.Millisecond)
	return got
}

func recordHeaders(rec *kgo.Record) map[string]string {
	h := make(map[string]string, len(rec.Headers))
	for _, hdr := range rec.Headers {
		h[hdr.Key] = string(hdr.Value)
	}
	return h
}

func adminClient(t *testing.T, brokers []string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}
