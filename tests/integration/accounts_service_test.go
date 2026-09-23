//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/repository"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestAccountsService_DepositStatementAndOutbox(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())

	acc := openAccount(t, ctx, svc, owner, usd)
	got, err := svc.GetAccount(ctx, owner, acc.ID)
	require.NoError(t, err)
	require.Equal(t, acc.ID, got.ID)
	list, err := svc.ListAccounts(ctx, owner)
	require.NoError(t, err)
	require.Len(t, list, 1)

	res, err := svc.Deposit(ctx, service.DepositCmd{
		Idem:        service.Idem{Key: idempotency.Namespace(owner.String(), "dep-1"), RequestHash: []byte("dep-1")},
		OwnerID:     owner,
		AccountID:   acc.ID,
		Amount:      money.New(10_000, usd),
		ExternalRef: "bank-wire",
	})
	require.NoError(t, err)
	require.Equal(t, int64(10_000), res.Account.Balance)

	lines, err := svc.Statement(ctx, owner, service.StatementFilter{AccountID: acc.ID, Limit: 10})
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Equal(t, int64(10_000), lines[0].Amount)
	require.Equal(t, int64(10_000), lines[0].BalanceAfter)
	require.Equal(t, "bank-wire", lines[0].Description)
	require.Equal(t, domain.EntryDeposit, lines[0].Kind)

	var events int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox WHERE event_type = 'account.credited' AND aggregate_id = $1`,
		acc.ID.String()).Scan(&events))
	require.Equal(t, 1, events)
}

func TestAccountsService_DepositReplay(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	acc := openAccount(t, ctx, svc, owner, usd)
	cmd := service.DepositCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "same"), RequestHash: []byte("body")},
		OwnerID:   owner,
		AccountID: acc.ID,
		Amount:    money.New(2500, usd),
	}

	ctx1, replayed := idempotency.WithReplayTracker(ctx)
	first, err := svc.Deposit(ctx1, cmd)
	require.NoError(t, err)
	require.False(t, replayed())

	ctx2, replayed := idempotency.WithReplayTracker(ctx)
	second, err := svc.Deposit(ctx2, cmd)
	require.NoError(t, err)
	require.True(t, replayed())
	require.Equal(t, first.EntryID, second.EntryID)

	var postings int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM postings WHERE account_id = $1`, acc.ID).Scan(&postings))
	require.Equal(t, 1, postings)
}

func TestAccountsService_SameKeyTwoOwners(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	for range 2 {
		owner := uuid.Must(uuid.NewV7())
		acc := openAccount(t, ctx, svc, owner, usd)
		_, err := svc.Deposit(ctx, service.DepositCmd{
			Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "1"), RequestHash: []byte("body")},
			OwnerID:   owner,
			AccountID: acc.ID,
			Amount:    money.New(100, usd),
		})
		require.NoError(t, err)
	}
}

func TestAccountsService_NotOwner(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	acc := openAccount(t, ctx, svc, owner, usd)
	other := uuid.Must(uuid.NewV7())
	_, err := svc.Deposit(ctx, service.DepositCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(other.String(), "x"), RequestHash: []byte("x")},
		OwnerID:   other,
		AccountID: acc.ID,
		Amount:    money.New(100, usd),
	})
	require.ErrorIs(t, err, domain.ErrNotAccountOwner)
	stored, err := svc.GetAccount(ctx, owner, acc.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), stored.Balance)
}

func TestAccountsService_WithdrawInsufficientKeepsKeyFree(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	acc := openAccount(t, ctx, svc, owner, usd)
	key := idempotency.Namespace(owner.String(), "w")
	_, err := svc.Withdraw(ctx, service.WithdrawCmd{
		Idem:      service.Idem{Key: key, RequestHash: []byte("w")},
		OwnerID:   owner,
		AccountID: acc.ID,
		Amount:    money.New(100, usd),
	})
	require.ErrorIs(t, err, domain.ErrInsufficientFunds)

	_, err = svc.Deposit(ctx, service.DepositCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "fund"), RequestHash: []byte("fund")},
		OwnerID:   owner,
		AccountID: acc.ID,
		Amount:    money.New(500, usd),
	})
	require.NoError(t, err)
	res, err := svc.Withdraw(ctx, service.WithdrawCmd{
		Idem:      service.Idem{Key: key, RequestHash: []byte("w")},
		OwnerID:   owner,
		AccountID: acc.ID,
		Amount:    money.New(100, usd),
	})
	require.NoError(t, err)
	require.Equal(t, int64(400), res.Account.Balance)
}

func TestAccountsService_ClosedRejectsDepositFrozenRejectsWithdraw(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())

	closed := openAccount(t, ctx, svc, owner, usd)
	_, err := pool.Exec(ctx, `UPDATE accounts SET status = 'closed' WHERE id = $1`, closed.ID)
	require.NoError(t, err)
	_, err = svc.Deposit(ctx, service.DepositCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "dep-closed"), RequestHash: []byte("dep-closed")},
		OwnerID:   owner,
		AccountID: closed.ID,
		Amount:    money.New(100, usd),
	})
	require.ErrorIs(t, err, domain.ErrAccountNotActive)
	require.Equal(t, int64(0), accountBalance(t, ctx, pool, closed.ID))

	frozen := fundAccount(t, ctx, svc, owner, usd, 500)
	_, err = pool.Exec(ctx, `UPDATE accounts SET status = 'frozen' WHERE id = $1`, frozen.ID)
	require.NoError(t, err)
	_, err = svc.Withdraw(ctx, service.WithdrawCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "wd-frozen"), RequestHash: []byte("wd-frozen")},
		OwnerID:   owner,
		AccountID: frozen.ID,
		Amount:    money.New(100, usd),
	})
	require.ErrorIs(t, err, domain.ErrAccountNotActive)
	require.Equal(t, int64(500), accountBalance(t, ctx, pool, frozen.ID))
}

func TestAccountsService_HoldCaptureAndRelease(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, svc, owner, usd, 5_000)
	dst := openAccount(t, ctx, svc, owner, usd)

	hold, err := svc.CreateHold(ctx, service.CreateHoldCmd{
		Idem:        service.Idem{Key: "svc:transfers/tr-1:hold", RequestHash: []byte("h")},
		AccountID:   src.ID,
		Amount:      money.New(2_000, usd),
		ReferenceID: "tr-1",
		TTL:         domain.DefaultHoldTTL,
	})
	require.NoError(t, err)
	again, err := svc.CreateHold(ctx, service.CreateHoldCmd{
		Idem:        service.Idem{Key: "svc:transfers/tr-1:hold-dup", RequestHash: []byte("h2")},
		AccountID:   src.ID,
		Amount:      money.New(2_000, usd),
		ReferenceID: "tr-1",
		TTL:         domain.DefaultHoldTTL,
	})
	require.NoError(t, err)
	require.Equal(t, hold.ID, again.ID)
	stored, err := svc.GetAccount(ctx, owner, src.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2_000), stored.Held)
	require.Equal(t, int64(3_000), stored.Available())

	captured, err := svc.CaptureHold(ctx, service.CaptureHoldCmd{
		Idem:          service.Idem{Key: "svc:transfers/tr-1:capture", RequestHash: []byte("c")},
		HoldID:        hold.ID,
		DestAccountID: dst.ID,
		DestAmount:    money.New(2_000, usd),
		ReferenceID:   "tr-1",
	})
	require.NoError(t, err)
	require.Equal(t, domain.HoldCaptured, captured.Hold.Status)
	srcAfter, err := svc.GetAccount(ctx, owner, src.ID)
	require.NoError(t, err)
	require.Equal(t, int64(3_000), srcAfter.Balance)
	require.Equal(t, int64(0), srcAfter.Held)
	dstAfter, err := svc.GetAccount(ctx, owner, dst.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2_000), dstAfter.Balance)

	src2 := fundAccount(t, ctx, svc, owner, usd, 1_000)
	hold2, err := svc.CreateHold(ctx, service.CreateHoldCmd{
		Idem:        service.Idem{Key: "svc:transfers/tr-2:hold", RequestHash: []byte("h")},
		AccountID:   src2.ID,
		Amount:      money.New(400, usd),
		ReferenceID: "tr-2",
	})
	require.NoError(t, err)
	released, err := svc.ReleaseHold(ctx, service.ReleaseHoldCmd{
		Idem:   service.Idem{Key: "svc:transfers/tr-2:release", RequestHash: []byte("r")},
		HoldID: hold2.ID,
	})
	require.NoError(t, err)
	require.Equal(t, domain.HoldReleased, released.Status)
	src2After, err := svc.GetAccount(ctx, owner, src2.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1_000), src2After.Balance)
	require.Equal(t, int64(0), src2After.Held)
	require.Equal(t, int64(1_000), src2After.Available())
}

func TestAccountsService_CaptureExpiredAndExpireHolds(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())
	src := fundAccount(t, ctx, svc, owner, usd, 1_000)
	dst := openAccount(t, ctx, svc, owner, usd)
	hold, err := svc.CreateHold(ctx, service.CreateHoldCmd{
		Idem:        service.Idem{Key: "svc:transfers/tr-exp:hold", RequestHash: []byte("h")},
		AccountID:   src.ID,
		Amount:      money.New(300, usd),
		ReferenceID: "tr-exp",
		TTL:         time.Millisecond,
	})
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	_, err = svc.CaptureHold(ctx, service.CaptureHoldCmd{
		Idem:          service.Idem{Key: "svc:transfers/tr-exp:capture", RequestHash: []byte("c")},
		HoldID:        hold.ID,
		DestAccountID: dst.ID,
		DestAmount:    money.New(300, usd),
		ReferenceID:   "tr-exp",
	})
	require.ErrorIs(t, err, domain.ErrHoldExpired)

	n, err := svc.ExpireHolds(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	after, err := svc.GetAccount(ctx, owner, src.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), after.Held)
	require.Equal(t, int64(1_000), after.Balance)
}

func TestConcurrentTransfers_PreserveTotal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	svc := newAccountsService(pool)
	usd := mustCurrency(t, "USD")
	owner := uuid.Must(uuid.NewV7())

	const (
		accounts = 10
		initial  = int64(10_000)
		workers  = 1000
	)
	ids := make([]uuid.UUID, accounts)
	for i := range accounts {
		acc := fundAccount(t, ctx, svc, owner, usd, initial)
		ids[i] = acc.ID
	}

	var wg sync.WaitGroup
	var failures atomic.Int32
	var firstErr atomic.Value
	wg.Add(workers)
	for i := range workers {
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i+1), 0x9e3779b97f4a7c15))
			a := rng.IntN(accounts)
			b := rng.IntN(accounts - 1)
			if b >= a {
				b++
			}
			amount := int64(rng.IntN(500) + 1)
			ref := uuid.Must(uuid.NewV7()).String()
			base := idempotency.Namespace("svc:transfers", "transfer:"+ref)
			hold, err := svc.CreateHold(ctx, service.CreateHoldCmd{
				Idem:        service.Idem{Key: base + ":hold", RequestHash: []byte(ref)},
				AccountID:   ids[a],
				Amount:      money.New(amount, usd),
				ReferenceID: ref,
				TTL:         domain.DefaultHoldTTL,
			})
			if errors.Is(err, domain.ErrInsufficientFunds) {
				return
			}
			if err != nil {
				if failures.Add(1) == 1 {
					firstErr.Store(err)
				}
				return
			}
			_, err = svc.CaptureHold(ctx, service.CaptureHoldCmd{
				Idem:          service.Idem{Key: base + ":capture", RequestHash: append([]byte(ref), 'c')},
				HoldID:        hold.ID,
				DestAccountID: ids[b],
				DestAmount:    money.New(amount, usd),
				ReferenceID:   ref,
			})
			if err != nil {
				if failures.Add(1) == 1 {
					firstErr.Store(err)
				}
			}
		}()
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		t.Fatalf("concurrent transfer failed: %v", v)
	}

	var customerSum, currencySum, bad int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(sum(balance), 0) FROM accounts WHERE owner_id = $1`, owner).Scan(&customerSum))
	require.Equal(t, accounts*initial, customerSum)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(sum(balance), 0) FROM accounts WHERE currency = 'USD'`).Scan(&currencySum))
	require.Equal(t, int64(0), currencySum)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM accounts WHERE owner_id = $1 AND (balance < 0 OR held <> 0)`, owner).Scan(&bad))
	require.Equal(t, int64(0), bad)
}

func newAccountsService(pool *pgxpool.Pool) *service.Service {
	return service.New(
		repository.New(),
		postgres.NewTxManager(pool),
		idempotency.NewStore(),
		outbox.NewWriter("accounts"),
		service.SystemClock{},
		service.UUIDv7{},
	)
}

func openAccount(t *testing.T, ctx context.Context, svc *service.Service, owner uuid.UUID, cur money.Currency) *domain.Account {
	t.Helper()
	acc, err := svc.CreateAccount(ctx, service.CreateAccountCmd{
		Idem:     service.Idem{Key: idempotency.Namespace(owner.String(), uuid.Must(uuid.NewV7()).String()), RequestHash: []byte("open")},
		OwnerID:  owner,
		Currency: cur,
	})
	require.NoError(t, err)
	return acc
}

func fundAccount(t *testing.T, ctx context.Context, svc *service.Service, owner uuid.UUID, cur money.Currency, amount int64) *domain.Account {
	t.Helper()
	acc := openAccount(t, ctx, svc, owner, cur)
	_, err := svc.Deposit(ctx, service.DepositCmd{
		Idem:      service.Idem{Key: idempotency.Namespace(owner.String(), "fund-"+acc.ID.String()), RequestHash: []byte(fmt.Sprintf("fund-%d", amount))},
		OwnerID:   owner,
		AccountID: acc.ID,
		Amount:    money.New(amount, cur),
	})
	require.NoError(t, err)
	return acc
}
