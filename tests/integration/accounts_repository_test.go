//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/repository"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestAccountsRepository_CreateGetList(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	earlier := time.Now().UTC().Truncate(time.Microsecond)
	later := earlier.Add(time.Second)

	first := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), owner, usd, earlier)
	second := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), owner, usd, later)
	require.NoError(t, repo.CreateAccount(ctx, pool, first))
	require.NoError(t, repo.CreateAccount(ctx, pool, second))

	got, err := repo.GetAccount(ctx, pool, first.ID)
	require.NoError(t, err)
	require.Equal(t, first.ID, got.ID)
	require.Equal(t, domain.KindCustomer, got.Kind)
	require.Equal(t, owner, got.OwnerID)
	require.Empty(t, got.Code)
	require.Equal(t, "USD", got.Currency.Code)
	require.Equal(t, domain.StatusActive, got.Status)
	require.Equal(t, int64(0), got.Balance)
	require.False(t, got.AllowOverdraft)
	require.WithinDuration(t, earlier, got.CreatedAt, time.Microsecond)

	_, err = repo.GetAccount(ctx, pool, uuid.Must(uuid.NewV7()))
	require.ErrorIs(t, err, domain.ErrAccountNotFound)

	list, err := repo.ListAccountsByOwner(ctx, pool, owner)
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, first.ID, list[0].ID)
	require.Equal(t, second.ID, list[1].ID)

	sysID, err := repo.SystemAccountID(ctx, pool, domain.SystemSettlement, usd)
	require.NoError(t, err)
	require.Equal(t, systemAccount(t, ctx, pool, "settlement.USD"), sysID)
	sys, err := repo.GetAccount(ctx, pool, sysID)
	require.NoError(t, err)
	require.Equal(t, domain.KindSystem, sys.Kind)
	require.Equal(t, uuid.Nil, sys.OwnerID)
	require.True(t, sys.AllowOverdraft)

	// A cache hit must not touch the database: a cancelled context would fail any query.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	again, err := repo.SystemAccountID(cancelled, pool, domain.SystemSettlement, usd)
	require.NoError(t, err)
	require.Equal(t, sysID, again)

	_, err = repo.SystemAccountID(ctx, pool, "nope", usd)
	require.ErrorIs(t, err, domain.ErrAccountNotFound)
}

func TestAccountsRepository_LockAccounts(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	a := createCustomer(t, ctx, pool, repo)
	b := createCustomer(t, ctx, pool, repo)

	locked, err := repo.LockAccounts(ctx, pool, []uuid.UUID{b.ID, a.ID, a.ID})
	require.NoError(t, err)
	require.Len(t, locked, 2)
	require.Equal(t, a.ID, locked[a.ID].ID)
	require.Equal(t, b.ID, locked[b.ID].ID)

	_, err = repo.LockAccounts(ctx, pool, []uuid.UUID{a.ID, uuid.Must(uuid.NewV7())})
	require.ErrorIs(t, err, domain.ErrAccountNotFound)
}

func TestAccountsRepository_LockOrderNoDeadlock(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	txm := postgres.NewTxManager(pool)
	a := createCustomer(t, ctx, pool, repo)
	b := createCustomer(t, ctx, pool, repo)

	const rounds = 200
	orders := [][]uuid.UUID{{a.ID, b.ID}, {b.ID, a.ID}}
	var wg sync.WaitGroup
	errs := make([]error, len(orders))
	wg.Add(len(orders))
	for i, ids := range orders {
		go func() {
			defer wg.Done()
			for range rounds {
				errs[i] = txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
					if _, err := repo.LockAccounts(ctx, tx, ids); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, `SELECT pg_sleep(0.001)`)
					return err
				})
				if errs[i] != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}

func TestAccountsRepository_UnappliedEntry(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	usd := mustCurrency(t, "USD")
	id := uuid.Must(uuid.NewV7())
	err := repo.InsertEntry(ctx, pool, &domain.JournalEntry{
		ID:            id,
		Kind:          domain.EntryDeposit,
		ReferenceType: "deposit",
		ReferenceID:   "unapplied",
		Postings: []domain.Posting{
			{AccountID: uuid.Must(uuid.NewV7()), Amount: -1, Currency: usd},
			{AccountID: uuid.Must(uuid.NewV7()), Amount: 1, Currency: usd},
		},
	})
	require.ErrorIs(t, err, domain.ErrEntryNotApplied)
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM journal_entries WHERE id = $1`, id).Scan(&n))
	require.Equal(t, 0, n)
}

func TestAccountsRepository_EntryAndStatement(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	txm := postgres.NewTxManager(pool)
	usd := mustCurrency(t, "USD")
	customer := createCustomer(t, ctx, pool, repo)
	settlementID, err := repo.SystemAccountID(ctx, pool, domain.SystemSettlement, usd)
	require.NoError(t, err)

	const n = 25
	now := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		locked, err := repo.LockAccounts(ctx, tx, []uuid.UUID{customer.ID, settlementID})
		if err != nil {
			return err
		}
		for i := 1; i <= n; i++ {
			if err := postDeposit(ctx, tx, repo, locked, customer.ID, settlementID, usd, now, uuid.Must(uuid.NewV7()).String()); err != nil {
				return err
			}
		}
		return repo.UpdateBalances(ctx, tx, locked[customer.ID], locked[settlementID])
	}))

	var page []service.StatementLine
	var cursor int64
	for {
		chunk, err := repo.Statement(ctx, pool, service.StatementFilter{
			AccountID: customer.ID,
			BeforeID:  cursor,
			Limit:     10,
		})
		require.NoError(t, err)
		if len(chunk) == 0 {
			break
		}
		page = append(page, chunk...)
		cursor = chunk[len(chunk)-1].PostingID
		if len(chunk) < 10 {
			break
		}
	}
	require.Len(t, page, n)
	seen := make(map[int64]struct{}, len(page))
	for i, line := range page {
		_, ok := seen[line.PostingID]
		require.False(t, ok)
		seen[line.PostingID] = struct{}{}
		if i > 0 {
			require.Less(t, line.PostingID, page[i-1].PostingID)
		}
		require.Equal(t, domain.EntryDeposit, line.Kind)
		require.Equal(t, int64(100), line.Amount)
	}
	require.Equal(t, int64(n*100), page[0].BalanceAfter)

	future := time.Now().UTC().Add(time.Hour)
	empty, err := repo.Statement(ctx, pool, service.StatementFilter{
		AccountID: customer.ID,
		From:      &future,
		Limit:     10,
	})
	require.NoError(t, err)
	require.Empty(t, empty)

	stored, err := repo.GetAccount(ctx, pool, customer.ID)
	require.NoError(t, err)
	require.Equal(t, int64(n*100), stored.Balance)
	require.Equal(t, int64(n), stored.Version)
}

func TestAccountsRepository_DuplicateEntryReference(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	txm := postgres.NewTxManager(pool)
	usd := mustCurrency(t, "USD")
	customer := createCustomer(t, ctx, pool, repo)
	settlementID, err := repo.SystemAccountID(ctx, pool, domain.SystemSettlement, usd)
	require.NoError(t, err)
	now := time.Now().UTC()

	require.NoError(t, txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		locked, err := repo.LockAccounts(ctx, tx, []uuid.UUID{customer.ID, settlementID})
		if err != nil {
			return err
		}
		if err := postDeposit(ctx, tx, repo, locked, customer.ID, settlementID, usd, now, "same-ref"); err != nil {
			return err
		}
		return repo.UpdateBalances(ctx, tx, locked[customer.ID], locked[settlementID])
	}))
	err = txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		locked, err := repo.LockAccounts(ctx, tx, []uuid.UUID{customer.ID, settlementID})
		if err != nil {
			return err
		}
		return postDeposit(ctx, tx, repo, locked, customer.ID, settlementID, usd, now, "same-ref")
	})
	require.ErrorIs(t, err, service.ErrEntryReferenceExists)
}

func TestAccountsRepository_LockExpiredHoldsDisjoint(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	repo := repository.New()
	txm := postgres.NewTxManager(pool)
	account := createCustomer(t, ctx, pool, repo)
	now := time.Now().UTC().Truncate(time.Microsecond)

	const expiredN = 4
	for i := range expiredN {
		require.NoError(t, repo.CreateHold(ctx, pool, &domain.Hold{
			ID:          uuid.Must(uuid.NewV7()),
			AccountID:   account.ID,
			Amount:      100,
			Status:      domain.HoldActive,
			ReferenceID: uuid.Must(uuid.NewV7()).String(),
			ExpiresAt:   now.Add(-time.Minute - time.Duration(i)*time.Second),
			CreatedAt:   now,
			UpdatedAt:   now,
		}))
	}
	future := &domain.Hold{
		ID:          uuid.Must(uuid.NewV7()),
		AccountID:   account.ID,
		Amount:      100,
		Status:      domain.HoldActive,
		ReferenceID: "still-active",
		ExpiresAt:   now.Add(time.Hour),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	require.NoError(t, repo.CreateHold(ctx, pool, future))
	err := repo.CreateHold(ctx, pool, &domain.Hold{
		ID:          uuid.Must(uuid.NewV7()),
		AccountID:   account.ID,
		Amount:      50,
		Status:      domain.HoldActive,
		ReferenceID: future.ReferenceID,
		ExpiresAt:   now.Add(time.Hour),
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	require.ErrorIs(t, err, service.ErrHoldReferenceExists)

	found, err := repo.FindHold(ctx, pool, account.ID, future.ReferenceID)
	require.NoError(t, err)
	require.Equal(t, future.ID, found.ID)
	_, err = repo.FindHold(ctx, pool, account.ID, "missing")
	require.ErrorIs(t, err, domain.ErrHoldNotFound)

	var (
		wg      sync.WaitGroup
		ready   sync.WaitGroup
		holding sync.WaitGroup
		got     [2][]*domain.Hold
		errs    [2]error
	)
	start := make(chan struct{})
	wg.Add(2)
	ready.Add(2)
	holding.Add(2)
	for i := range 2 {
		go func() {
			defer wg.Done()
			errs[i] = txm.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
				ready.Done()
				<-start
				holds, err := repo.LockExpiredHolds(ctx, tx, now, 10, nil)
				got[i] = holds
				holding.Done()
				if err != nil {
					return err
				}
				holding.Wait()
				return nil
			})
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[uuid.UUID]int)
	for _, batch := range got {
		for _, h := range batch {
			seen[h.ID]++
			require.NotEqual(t, future.ID, h.ID)
		}
	}
	require.Len(t, seen, expiredN)
	for _, n := range seen {
		require.Equal(t, 1, n)
	}
}

func postDeposit(ctx context.Context, tx pgx.Tx, repo *repository.Repository, locked map[uuid.UUID]*domain.Account, customer, settlement uuid.UUID, cur money.Currency, now time.Time, ref string) error {
	entry := &domain.JournalEntry{
		ID:            uuid.Must(uuid.NewV7()),
		Kind:          domain.EntryDeposit,
		ReferenceType: "deposit",
		ReferenceID:   ref,
		Description:   "deposit",
		CreatedAt:     now,
		Postings: []domain.Posting{
			{AccountID: settlement, Amount: -100, Currency: cur},
			{AccountID: customer, Amount: 100, Currency: cur},
		},
	}
	if err := entry.Apply(locked, now); err != nil {
		return err
	}
	return repo.InsertEntry(ctx, tx, entry)
}

func createCustomer(t *testing.T, ctx context.Context, pool *pgxpool.Pool, repo *repository.Repository) *domain.Account {
	t.Helper()
	a := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), mustCurrency(t, "USD"), time.Now().UTC())
	require.NoError(t, repo.CreateAccount(ctx, pool, a))
	return a
}

func mustCurrency(t *testing.T, code string) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency(code)
	require.NoError(t, err)
	return c
}
