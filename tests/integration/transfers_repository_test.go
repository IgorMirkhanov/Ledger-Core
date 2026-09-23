//go:build integration

package integration

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/repository"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestTransfersRepository_CreateGetFXAndList(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Transfers())
	repo := repository.New()
	usd := mustCurrency(t, "USD")
	rub := mustCurrency(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	now := time.Now().UTC().Truncate(time.Microsecond)

	rate, err := money.ParseRate("90.5")
	require.NoError(t, err)
	fx, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), owner, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(10_000, usd), rub, rate, now)
	require.NoError(t, err)
	require.NoError(t, repo.Create(ctx, pool, fx))

	got, err := repo.Get(ctx, pool, fx.ID)
	require.NoError(t, err)
	require.Equal(t, int64(10_000), got.Amount.Amount())
	require.Equal(t, "USD", got.Amount.Currency().Code)
	require.Equal(t, "RUB", got.DestAmount.Currency().Code)
	require.Equal(t, 0, rate.Cmp(got.FXRate))
	require.Equal(t, domain.StatusCreated, got.Status)
	require.Equal(t, int64(0), got.Version)

	seed, err := repo.GetRate(ctx, pool, usd, rub)
	require.NoError(t, err)
	want, err := money.ParseRate("90")
	require.NoError(t, err)
	require.Equal(t, 0, want.Cmp(seed))

	_, err = repo.GetRate(ctx, pool, usd, mustCurrency(t, "JPY"))
	require.ErrorIs(t, err, domain.ErrFXRateNotFound)
	_, err = repo.Get(ctx, pool, uuid.Must(uuid.NewV7()))
	require.ErrorIs(t, err, domain.ErrNotFound)

	same, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), owner, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(500, usd), usd, nil, now.Add(time.Second))
	require.NoError(t, err)
	require.NoError(t, repo.Create(ctx, pool, same))
	older, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), owner, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(100, usd), usd, nil, now.Add(-time.Second))
	require.NoError(t, err)
	require.NoError(t, repo.Create(ctx, pool, older))

	page, err := repo.ListByOwner(ctx, pool, owner, nil, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.Equal(t, same.ID, page[0].ID)
	require.Equal(t, fx.ID, page[1].ID)
	require.NotNil(t, page[1].FXRate)

	next, err := repo.ListByOwner(ctx, pool, owner, &service.ListCursor{CreatedAt: page[1].CreatedAt, ID: page[1].ID}, 2)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, older.ID, next[0].ID)
	require.Nil(t, next[0].FXRate)
}

func TestTransfersRepository_OptimisticUpdate(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Transfers())
	repo := repository.New()
	usd := mustCurrency(t, "USD")
	now := time.Now().UTC().Truncate(time.Microsecond)
	tr, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(100, usd), usd, nil, now)
	require.NoError(t, err)
	require.NoError(t, repo.Create(ctx, pool, tr))

	holdID := uuid.Must(uuid.NewV7())
	require.NoError(t, tr.OnHoldCreated(holdID, now.Add(time.Second)))
	require.NoError(t, repo.Update(ctx, pool, tr))
	require.ErrorIs(t, repo.Update(ctx, pool, tr), service.ErrConcurrentUpdate)

	got, err := repo.Get(ctx, pool, tr.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusFundsHeld, got.Status)
	require.Equal(t, holdID, got.HoldID)
	require.Equal(t, int64(1), got.Version)

	entryID := uuid.Must(uuid.NewV7())
	require.NoError(t, tr.OnCaptured(entryID, now.Add(2*time.Second)))
	require.NoError(t, repo.Update(ctx, pool, tr))
	require.NoError(t, repo.AppendStep(ctx, pool, service.StepLog{
		TransferID: tr.ID,
		Step:       domain.StepCapture,
		Outcome:    "ok",
		Duration:   12 * time.Millisecond,
	}))

	var steps int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM transfer_steps WHERE transfer_id = $1`, tr.ID).Scan(&steps))
	require.Equal(t, 1, steps)
}

func TestTransfersRepository_ClaimPendingSkipLocked(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Transfers())
	repo := repository.New()
	usd := mustCurrency(t, "USD")
	base := time.Now().UTC().Truncate(time.Microsecond)

	var pending []uuid.UUID
	for i := range 3 {
		at := base.Add(-time.Duration(3-i) * time.Second)
		tr, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
			money.New(100, usd), usd, nil, at)
		require.NoError(t, err)
		tr.NextAttemptAt = &at
		require.NoError(t, repo.Create(ctx, pool, tr))
		pending = append(pending, tr.ID)
	}

	future := base.Add(time.Hour)
	later, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(100, usd), usd, nil, base)
	require.NoError(t, err)
	later.NextAttemptAt = &future
	require.NoError(t, repo.Create(ctx, pool, later))

	done, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		money.New(100, usd), usd, nil, base.Add(-time.Minute))
	require.NoError(t, err)
	require.NoError(t, done.OnHoldCreated(uuid.Must(uuid.NewV7()), base))
	require.NoError(t, done.OnCaptured(uuid.Must(uuid.NewV7()), base))
	require.NoError(t, repo.Create(ctx, pool, done))

	tx1, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx1.Rollback(context.Background()) })
	first, err := repo.ClaimPending(ctx, tx1, base, 2)
	require.NoError(t, err)
	require.Equal(t, pending[:2], first)

	claimCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx2, err := pool.Begin(claimCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx2.Rollback(context.Background()) })
	second, err := repo.ClaimPending(claimCtx, tx2, base, 10)
	require.NoError(t, err)
	require.Equal(t, pending[2:], second)
}

func TestTransfersRepository_FXRateRoundTrip(t *testing.T) {
	rate := big.NewRat(905, 10)
	require.Equal(t, "90.5000000000", rate.FloatString(10))
	back, ok := new(big.Rat).SetString(rate.FloatString(10))
	require.True(t, ok)
	require.Equal(t, 0, rate.Cmp(back))
}
