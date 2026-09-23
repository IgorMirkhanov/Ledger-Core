//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestIdempotency_ReplayReturnsStoredResponse(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	store := idempotency.NewStore()
	txm := postgres.NewTxManager(pool)
	hash := []byte("hash-v1")
	want := []byte(`{"id":"1"}`)

	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := store.Begin(ctx, tx, "accounts.Deposit", "key-1", hash)
		if err != nil {
			return err
		}
		if rec != nil {
			return errors.New("first begin returned a record")
		}
		return store.Complete(ctx, tx, "accounts.Deposit", "key-1", want, "OK")
	}))

	var rec *idempotency.Record
	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		rec, err = store.Begin(ctx, tx, "accounts.Deposit", "key-1", hash)
		return err
	}))
	require.NotNil(t, rec)
	require.Equal(t, want, rec.Response)
	require.Equal(t, "OK", rec.ResponseCode)
}

func TestIdempotency_DifferentHashReused(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	store := idempotency.NewStore()
	txm := postgres.NewTxManager(pool)

	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := store.Begin(ctx, tx, "accounts.Deposit", "key-1", []byte("hash-a"))
		if err != nil {
			return err
		}
		if rec != nil {
			return errors.New("first begin returned a record")
		}
		return store.Complete(ctx, tx, "accounts.Deposit", "key-1", []byte("ok"), "OK")
	}))

	err := txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := store.Begin(ctx, tx, "accounts.Deposit", "key-1", []byte("hash-b"))
		return err
	})
	require.ErrorIs(t, err, idempotency.ErrKeyReused)
}

func TestIdempotency_RollbackDropsKey(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	store := idempotency.NewStore()
	txm := postgres.NewTxManager(pool)
	hash := []byte("hash-v1")

	err := txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := store.Begin(ctx, tx, "accounts.Deposit", "key-1", hash)
		if err != nil {
			return err
		}
		if rec != nil {
			return errors.New("first begin returned a record")
		}
		return errors.New("rollback")
	})
	require.Error(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys`).Scan(&n))
	require.Equal(t, 0, n)

	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rec, err := store.Begin(ctx, tx, "accounts.Deposit", "key-1", hash)
		if err != nil {
			return err
		}
		if rec != nil {
			return errors.New("key survived rollback")
		}
		return store.Complete(ctx, tx, "accounts.Deposit", "key-1", []byte("ok"), "OK")
	}))
}

func TestIdempotency_ConcurrentSingleSideEffect(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	_, err := pool.Exec(ctx, `CREATE TABLE side_effects (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`)
	require.NoError(t, err)

	store := idempotency.NewStore()
	txm := postgres.NewTxManager(pool)
	const (
		scope = "it.SideEffect"
		key   = "same-key"
		n     = 20
	)
	hash := []byte("hash-v1")

	var (
		wg       sync.WaitGroup
		ready    sync.WaitGroup
		replayed atomic.Int32
		errs     [n]error
	)
	start := make(chan struct{})
	wg.Add(n)
	ready.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			errs[i] = txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
				rec, err := store.Begin(ctx, tx, scope, key, hash)
				if err != nil {
					return err
				}
				if rec != nil {
					replayed.Add(1)
					return nil
				}
				if _, err := tx.Exec(ctx, `INSERT INTO side_effects DEFAULT VALUES`); err != nil {
					return err
				}
				// Hold the unique-index lock long enough for the other 19
				// transactions to block in Begin before this one commits.
				if _, err := tx.Exec(ctx, `SELECT pg_sleep(0.2)`); err != nil {
					return err
				}
				return store.Complete(ctx, tx, scope, key, []byte("ok"), "OK")
			})
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	var rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM side_effects`).Scan(&rows))
	require.Equal(t, 1, rows)
	require.Equal(t, int32(n-1), replayed.Load())
}
