//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IgorMirkhanov/ledger-core/internal/notifications"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestNotifications_DedupAndDLQ(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Notifications())
	brokers := testenv.StartRedpanda(t)
	topic := topicName(t)
	dlq := topic + ".dlq"
	ensureTopic(t, brokers, topic)
	ensureTopic(t, brokers, dlq)

	handler := notifications.NewHandler(pool)
	runConsumer(t, brokers, topic, dlq, 5, handler.Handle)

	owner := uuid.Must(uuid.NewV7())
	eventID := uuid.Must(uuid.NewV7())
	env := outbox.Envelope{
		EventID:       eventID,
		EventType:     "account.credited",
		SchemaVersion: 1,
		AggregateType: "account",
		AggregateID:   uuid.Must(uuid.NewV7()).String(),
		OccurredAt:    time.Now().UTC(),
		Producer:      "it",
		Data: mustJSON(t, map[string]any{
			"account_id": uuid.Must(uuid.NewV7()).String(), "owner_id": owner.String(),
			"entry_id": uuid.Must(uuid.NewV7()).String(), "amount": int64(1000), "currency": "USD",
			"balance_after": int64(1000), "entry_kind": "deposit",
		}),
	}
	body, err := json.Marshal(env)
	require.NoError(t, err)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	prodCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for range 3 {
		require.NoError(t, cl.ProduceSync(prodCtx, &kgo.Record{
			Topic: topic, Key: []byte(env.AggregateID), Value: body,
		}).FirstErr())
	}

	require.Eventually(t, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM notifications`).Scan(&n)
		return n == 1
	}, 20*time.Second, 50*time.Millisecond)

	var processed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, eventID).Scan(&processed))
	require.Equal(t, 1, processed)

	require.NoError(t, cl.ProduceSync(prodCtx, &kgo.Record{
		Topic: topic, Key: []byte("bad"), Value: []byte(`{not-json`),
	}).FirstErr())

	rec := consumeOne(t, brokers, dlq)
	require.Equal(t, `{not-json`, string(rec.Value))
	require.Contains(t, recordHeaders(rec), "x-error")
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
