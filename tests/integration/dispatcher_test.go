//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/notifications"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestDispatcher_RetryBackoffThenSent(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Notifications())
	id := insertPendingNotification(t, ctx, pool)

	var calls atomic.Int32
	sender := &scriptedSender{fn: func(context.Context, notifications.Notification, string) error {
		n := calls.Add(1)
		if n <= 2 {
			return errors.New("provider down")
		}
		return nil
	}}

	clock := &fakeClock{t: time.Now().UTC()}
	d, err := notifications.NewDispatcher(pool, sender, nil)
	require.NoError(t, err)
	d.ConfigureForTest(time.Minute, func(int) time.Duration { return time.Second }, clock.Now)

	require.NoError(t, d.Tick(ctx))
	require.Equal(t, int32(1), calls.Load())
	requireNotification(t, ctx, pool, id, "pending", 1)

	// Lease still active — no second attempt yet.
	require.NoError(t, d.Tick(ctx))
	require.Equal(t, int32(1), calls.Load())

	// Advance past next_attempt_at (lease was overwritten by backoff=1s).
	clock.Advance(2 * time.Second)
	require.NoError(t, d.Tick(ctx))
	require.Equal(t, int32(2), calls.Load())
	requireNotification(t, ctx, pool, id, "pending", 2)

	clock.Advance(2 * time.Second)
	require.NoError(t, d.Tick(ctx))
	require.Equal(t, int32(3), calls.Load())
	requireNotification(t, ctx, pool, id, "sent", 3)
}

func TestDispatcher_LeaseBlocksOtherReplica(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Notifications())
	id := insertPendingNotification(t, ctx, pool)

	block := make(chan struct{})
	release := make(chan struct{})
	var started atomic.Bool
	sender := &scriptedSender{fn: func(context.Context, notifications.Notification, string) error {
		started.Store(true)
		close(block)
		<-release
		return nil
	}}

	d1, err := notifications.NewDispatcher(pool, sender, nil)
	require.NoError(t, err)
	d1.ConfigureForTest(time.Minute, notifications.SendBackoff, time.Now)

	d2, err := notifications.NewDispatcher(pool, &scriptedSender{}, nil)
	require.NoError(t, err)
	d2.ConfigureForTest(time.Minute, notifications.SendBackoff, time.Now)

	done := make(chan error, 1)
	go func() { done <- d1.Tick(ctx) }()

	select {
	case <-block:
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not start")
	}

	// While d1 holds the lease and is mid-Send, d2 must not claim the same row.
	require.NoError(t, d2.Tick(ctx))
	var status string
	var attempts int
	require.NoError(t, pool.QueryRow(ctx, `SELECT status, attempts FROM notifications WHERE id = $1`, id).Scan(&status, &attempts))
	require.Equal(t, "pending", status)
	require.Equal(t, 0, attempts)

	close(release)
	require.NoError(t, <-done)
	requireNotification(t, ctx, pool, id, "sent", 1)
}

func TestDispatcher_OneFailureDoesNotBlockBatch(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Notifications())
	bad := insertPendingNotification(t, ctx, pool)
	good := insertPendingNotification(t, ctx, pool)

	sender := &scriptedSender{fn: func(_ context.Context, n notifications.Notification, _ string) error {
		if n.ID == bad {
			return errors.New("boom")
		}
		return nil
	}}
	d, err := notifications.NewDispatcher(pool, sender, nil)
	require.NoError(t, err)
	d.ConfigureForTest(time.Minute, func(int) time.Duration { return time.Hour }, time.Now)

	require.NoError(t, d.Tick(ctx))
	require.NoError(t, d.Tick(ctx)) // second pass if first batch ordering was unlucky
	require.Eventually(t, func() bool {
		var status string
		var attempts int
		_ = pool.QueryRow(ctx, `SELECT status::text, attempts FROM notifications WHERE id = $1`, good).Scan(&status, &attempts)
		return status == "sent" && attempts == 1
	}, 3*time.Second, 20*time.Millisecond)
	requireNotification(t, ctx, pool, bad, "pending", 1)
	requireNotification(t, ctx, pool, good, "sent", 1)
}

type scriptedSender struct {
	fn func(context.Context, notifications.Notification, string) error
	mu sync.Mutex
}

func (s *scriptedSender) Send(ctx context.Context, n notifications.Notification, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fn == nil {
		return nil
	}
	return s.fn(ctx, n, body)
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func insertPendingNotification(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	eventID := uuid.Must(uuid.NewV7())
	notifID := uuid.Must(uuid.NewV7())
	userID := uuid.Must(uuid.NewV7())
	_, err := pool.Exec(ctx, `
		INSERT INTO processed_events (event_id, event_type, topic, partition, "offset")
		VALUES ($1, 'account.credited', 'ledger.accounts.v1', 0, 0)`, eventID)
	require.NoError(t, err)
	payload, err := json.Marshal(map[string]any{
		"account_id": uuid.Must(uuid.NewV7()).String(), "owner_id": userID.String(),
		"amount": 100, "currency": "USD", "entry_kind": "deposit",
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO notifications (id, user_id, event_id, channel, template, payload, status, next_attempt_at)
		VALUES ($1, $2, $3, 'email', 'deposit_received', $4, 'pending', now())`,
		notifID, userID, eventID, payload)
	require.NoError(t, err)
	return notifID
}

func requireNotification(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, status string, attempts int) {
	t.Helper()
	var gotStatus string
	var gotAttempts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status::text, attempts FROM notifications WHERE id = $1`, id,
	).Scan(&gotStatus, &gotAttempts))
	require.Equal(t, status, gotStatus)
	require.Equal(t, attempts, gotAttempts)
}
