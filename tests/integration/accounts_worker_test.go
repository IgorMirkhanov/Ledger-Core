//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/worker"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestJanitor_DeletesExpiredKeysAndOldOutbox(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())

	_, err := pool.Exec(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_hash, expires_at)
		VALUES ('it', 'old', '\x01', now() - interval '1 hour'),
		       ('it', 'fresh', '\x01', now() + interval '1 hour')`)
	require.NoError(t, err)
	oldID := uuid.Must(uuid.NewV7())
	freshID := uuid.Must(uuid.NewV7())
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox (event_id, topic, aggregate_type, aggregate_id, event_type, payload, published_at)
		VALUES ($1, 't', 'account', 'a', 'account.credited', '{}', now() - interval '8 days'),
		       ($2, 't', 'account', 'a', 'account.credited', '{}', now())`, oldID, freshID)
	require.NoError(t, err)

	require.NoError(t, worker.NewJanitor(pool, 7*24*time.Hour).Sweep(ctx))

	var keys, rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE scope = 'it'`).Scan(&keys))
	require.Equal(t, 1, keys)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_id = $1`, oldID).Scan(&rows))
	require.Equal(t, 0, rows)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_id = $1`, freshID).Scan(&rows))
	require.Equal(t, 1, rows)
}
