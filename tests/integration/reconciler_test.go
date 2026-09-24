//go:build integration

package integration

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/reconciler"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestReconciler_CleanLedger(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())

	// Balanced deposit: customer +100, settlement -100.
	customer := insertCustomer(t, ctx, pool, "USD")
	system := systemAccount(t, ctx, pool, "settlement.USD")
	entryID := uuid.Must(uuid.NewV7())
	require.NoError(t, postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := insertJournal(ctx, tx, entryID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
			VALUES ($1, $2, 10000, 'USD', 10000)`, entryID, customer); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
			VALUES ($1, $2, -10000, 'USD', -10000)`, entryID, system); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = 10000, version = version + 1 WHERE id = $1`, customer); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE accounts SET balance = -10000, version = version + 1 WHERE id = $1`, system)
		return err
	}))

	res, err := reconciler.Run(ctx, pool, reconciler.DefaultChecks(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Status)
	require.Empty(t, res.Discrepancies)
}

func TestReconciler_FindsAnomalies(t *testing.T) {
	t.Run("g9", func(t *testing.T) {
		ctx := testContext(t)
		pool := testenv.MigratedPool(t, migrations.Accounts())
		system := systemAccount(t, ctx, pool, "settlement.USD")
		_, err := pool.Exec(ctx, `UPDATE accounts SET balance = 42 WHERE id = $1`, system)
		require.NoError(t, err)

		res, err := reconciler.Run(ctx, pool, []reconciler.Check{reconciler.DefaultChecks()[2]}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		require.Equal(t, "discrepancies", res.Status)
		require.NotEmpty(t, res.Discrepancies)
		require.Equal(t, "g9_balance_equals_postings", res.Discrepancies[0].CheckName)
	})

	t.Run("g2", func(t *testing.T) {
		ctx := testContext(t)
		pool := testenv.MigratedPool(t, migrations.Accounts())
		system := systemAccount(t, ctx, pool, "settlement.USD")
		_, err := pool.Exec(ctx, `UPDATE accounts SET balance = 7 WHERE id = $1`, system)
		require.NoError(t, err)

		res, err := reconciler.Run(ctx, pool, []reconciler.Check{reconciler.DefaultChecks()[1]}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		require.Equal(t, "discrepancies", res.Status)
		require.Equal(t, "g2_currency_zero_sum", res.Discrepancies[0].CheckName)
	})

	t.Run("g1", func(t *testing.T) {
		ctx := testContext(t)
		pool := testenv.MigratedPool(t, migrations.Accounts())
		customer := insertCustomer(t, ctx, pool, "USD")
		system := systemAccount(t, ctx, pool, "settlement.USD")

		_, err := pool.Exec(ctx, `ALTER TABLE postings DISABLE TRIGGER USER`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `ALTER TABLE postings ENABLE TRIGGER USER`)
		})

		entryID := uuid.Must(uuid.NewV7())
		require.NoError(t, postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			return insertPostings(ctx, tx, entryID, customer, system, 10000, -4000, "USD", "USD")
		}))

		res, err := reconciler.Run(ctx, pool, []reconciler.Check{reconciler.DefaultChecks()[0]}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		require.Equal(t, "discrepancies", res.Status)
		require.Equal(t, "g1_journal_balanced", res.Discrepancies[0].CheckName)
	})

	t.Run("g3", func(t *testing.T) {
		ctx := testContext(t)
		pool := testenv.MigratedPool(t, migrations.Accounts())
		customer := insertCustomer(t, ctx, pool, "USD")
		// Active hold without updating accounts.held.
		_, err := pool.Exec(ctx, `
			INSERT INTO holds (id, account_id, amount, status, reference_id, expires_at)
			VALUES ($1, $2, 500, 'active', 'ref', now() + interval '1 hour')`,
			uuid.Must(uuid.NewV7()), customer)
		require.NoError(t, err)

		res, err := reconciler.Run(ctx, pool, []reconciler.Check{reconciler.DefaultChecks()[3]}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		require.Equal(t, "discrepancies", res.Status)
		require.Equal(t, "g3_held_equals_active_holds", res.Discrepancies[0].CheckName)
	})

	t.Run("expired_holds", func(t *testing.T) {
		ctx := testContext(t)
		pool := testenv.MigratedPool(t, migrations.Accounts())
		customer := insertCustomer(t, ctx, pool, "USD")
		holdID := uuid.Must(uuid.NewV7())
		_, err := pool.Exec(ctx, `
			INSERT INTO holds (id, account_id, amount, status, reference_id, expires_at)
			VALUES ($1, $2, 100, 'active', 'old', $3)`,
			holdID, customer, time.Now().UTC().Add(-10*time.Minute))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE accounts SET held = 100, balance = 100 WHERE id = $1`, customer)
		require.NoError(t, err)

		res, err := reconciler.Run(ctx, pool, []reconciler.Check{reconciler.DefaultChecks()[4]}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		require.Equal(t, "discrepancies", res.Status)
		require.Equal(t, "expired_active_holds", res.Discrepancies[0].CheckName)
	})
}
