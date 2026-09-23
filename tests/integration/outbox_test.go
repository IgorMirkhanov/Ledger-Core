//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestOutbox_AddRollsBackWithTransaction(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())

	err := postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := outbox.NewWriter("accounts").Add(ctx, tx, sampleEvent("ledger.accounts.v1", "acc-1")); err != nil {
			return err
		}
		return errors.New("rollback")
	})
	require.Error(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n))
	require.Equal(t, 0, n)
}

func TestOutbox_RelayPublishesInOrder(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	keys := []string{"acc-a", "acc-b", "acc-c"}
	for _, key := range keys {
		require.NoError(t, addOutboxEvent(ctx, pool, "ledger.accounts.v1", key))
	}

	pub := &recordingPublisher{}
	startRelay(t, pool, t.Name(), pub)

	require.Eventually(t, func() bool {
		return len(pub.snapshot()) == len(keys)
	}, 10*time.Second, 20*time.Millisecond)

	got := pub.snapshot()
	require.Len(t, got, len(keys))
	for i, msg := range got {
		require.Equal(t, keys[i], string(msg.Key))
		if i > 0 {
			require.Greater(t, msg.ID, got[i-1].ID)
		}
	}
	requireNonePending(t, ctx, pool)
}

func TestOutbox_RelayRecordsPublishFailure(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	require.NoError(t, addOutboxEvent(ctx, pool, "ledger.accounts.v1", "acc-1"))

	startRelay(t, pool, t.Name(), &recordingPublisher{err: errors.New("boom")})

	require.Eventually(t, func() bool {
		var (
			publishedAt *time.Time
			attempts    int
			lastError   *string
		)
		err := pool.QueryRow(ctx,
			`SELECT published_at, attempts, last_error FROM outbox`,
		).Scan(&publishedAt, &attempts, &lastError)
		if err != nil || publishedAt != nil || attempts < 1 || lastError == nil {
			return false
		}
		return *lastError != ""
	}, 10*time.Second, 20*time.Millisecond)
}

func TestOutbox_SingleLeader(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	keys := []string{"acc-a", "acc-b", "acc-c"}
	for _, key := range keys {
		require.NoError(t, addOutboxEvent(ctx, pool, "ledger.accounts.v1", key))
	}

	pubA := &recordingPublisher{}
	pubB := &recordingPublisher{}
	startRelay(t, pool, t.Name(), pubA)
	startRelay(t, pool, t.Name(), pubB)

	require.Eventually(t, func() bool {
		a, b := len(pubA.snapshot()), len(pubB.snapshot())
		if a+b != len(keys) {
			return false
		}
		return (a == len(keys) && b == 0) || (b == len(keys) && a == 0)
	}, 10*time.Second, 20*time.Millisecond)

	winner := pubA.snapshot()
	if len(winner) == 0 {
		winner = pubB.snapshot()
	}
	require.Len(t, winner, len(keys))
	for i, msg := range winner {
		require.Equal(t, keys[i], string(msg.Key))
		if i > 0 {
			require.Greater(t, msg.ID, winner[i-1].ID)
		}
	}
	requireNonePending(t, ctx, pool)
}

func TestOutbox_RelayPublishesToRedpanda(t *testing.T) {
	// Image pull can exceed the per-test deadline; start the broker on its own clock first.
	brokers := testenv.StartRedpanda(t)
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	const (
		topic       = "ledger.it.accounts"
		aggregateID = "acc-redpanda"
	)
	ensureTopic(t, brokers, topic)

	prod, err := kafka.NewProducer(brokers, "outbox-it")
	require.NoError(t, err)
	t.Cleanup(func() { _ = prod.Close(context.Background()) })

	require.NoError(t, addOutboxEvent(ctx, pool, topic, aggregateID))
	startRelay(t, pool, t.Name(), prod)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	var got *kgo.Record
	require.Eventually(t, func() bool {
		pollCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		fetches := cl.PollFetches(pollCtx)
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == aggregateID {
				got = r
			}
		})
		return got != nil
	}, 30*time.Second, 50*time.Millisecond)

	require.Equal(t, aggregateID, string(got.Key))
	var env outbox.Envelope
	require.NoError(t, json.Unmarshal(got.Value, &env))
	require.Equal(t, aggregateID, env.AggregateID)
	require.Equal(t, "account.credited", env.EventType)
	requireNonePending(t, ctx, pool)
}

func sampleEvent(topic, aggregateID string) outbox.Event {
	return outbox.Event{
		Topic:         topic,
		AggregateType: "account",
		AggregateID:   aggregateID,
		EventType:     "account.credited",
		Payload:       map[string]string{"account_id": aggregateID},
	}
}

func addOutboxEvent(ctx context.Context, pool *pgxpool.Pool, topic, aggregateID string) error {
	w := outbox.NewWriter("accounts")
	return postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return w.Add(ctx, tx, sampleEvent(topic, aggregateID))
	})
}

func startRelay(t *testing.T, pool *pgxpool.Pool, service string, pub outbox.Publisher) {
	t.Helper()
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{
		ServiceName:  service,
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
	}, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = relay.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func requireNonePending(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n))
	require.Equal(t, 0, n)
}

func ensureTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).CreateTopics(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, resp.Error())
}

type recordingPublisher struct {
	mu   sync.Mutex
	msgs []outbox.Message
	err  error
}

func (p *recordingPublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	if p.err != nil {
		return p.err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func (p *recordingPublisher) snapshot() []outbox.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]outbox.Message, len(p.msgs))
	copy(out, p.msgs)
	return out
}
