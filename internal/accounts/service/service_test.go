package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
)

func TestDeposit_ReplayDoesNotTouchRepositoryAgain(t *testing.T) {
	repo := newFakeRepo()
	usd := mustUSD(t)
	owner := uuid.Must(uuid.NewV7())
	customer := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), owner, usd, time.Unix(0, 0).UTC())
	settlement := &domain.Account{
		ID: uuid.Must(uuid.NewV7()), Kind: domain.KindSystem, Code: "settlement.USD",
		Currency: usd, Status: domain.AccountStatus("active"), AllowOverdraft: true,
	}
	repo.putAccount(customer)
	repo.putAccount(settlement)

	idem := newMemIdem()
	svc := New(repo, fakeTx{}, idem, &memOutbox{}, fixedClock(time.Unix(100, 0).UTC()), UUIDv7{})
	cmd := DepositCmd{
		Idem:      Idem{Key: "owner/k", RequestHash: []byte("hash-1")},
		OwnerID:   owner,
		AccountID: customer.ID,
		Amount:    money.New(1500, usd),
	}
	ctx, replayed := idempotency.WithReplayTracker(context.Background())
	first, err := svc.Deposit(ctx, cmd)
	require.NoError(t, err)
	require.False(t, replayed())
	require.Equal(t, int64(1500), first.Account.Balance)

	ctx, replayed = idempotency.WithReplayTracker(context.Background())
	second, err := svc.Deposit(ctx, cmd)
	require.NoError(t, err)
	require.True(t, replayed())
	require.Equal(t, first.EntryID, second.EntryID)
	require.Equal(t, int64(1500), second.Account.Balance)
	require.Equal(t, 1, repo.inserts)
}

func TestWithdraw_BusinessErrorDoesNotComplete(t *testing.T) {
	repo := newFakeRepo()
	usd := mustUSD(t)
	owner := uuid.Must(uuid.NewV7())
	customer := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), owner, usd, time.Unix(0, 0).UTC())
	customer.Balance = 100
	settlement := &domain.Account{
		ID: uuid.Must(uuid.NewV7()), Kind: domain.KindSystem, Code: "settlement.USD",
		Currency: usd, Status: domain.AccountStatus("active"), AllowOverdraft: true,
	}
	repo.putAccount(customer)
	repo.putAccount(settlement)
	idem := newMemIdem()
	svc := New(repo, fakeTx{}, idem, &memOutbox{}, fixedClock(time.Unix(100, 0).UTC()), UUIDv7{})

	_, err := svc.Withdraw(context.Background(), WithdrawCmd{
		Idem:      Idem{Key: "owner/w", RequestHash: []byte("h")},
		OwnerID:   owner,
		AccountID: customer.ID,
		Amount:    money.New(500, usd),
	})
	require.ErrorIs(t, err, domain.ErrInsufficientFunds)
	require.Equal(t, 0, idem.completes)
	require.Equal(t, 0, repo.inserts)
	require.Equal(t, int64(100), customer.Balance)
}

func TestCaptureHold_FXBuildsFourPostings(t *testing.T) {
	repo := newFakeRepo()
	usd := mustUSD(t)
	eur, err := money.ParseCurrency("EUR")
	require.NoError(t, err)
	src := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), usd, time.Unix(0, 0).UTC())
	src.Balance = 1000
	src.Held = 400
	dst := domain.NewCustomerAccount(uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), eur, time.Unix(0, 0).UTC())
	fxUSD := &domain.Account{
		ID: uuid.Must(uuid.NewV7()), Kind: domain.KindSystem, Code: "fx.USD",
		Currency: usd, Status: domain.AccountStatus("active"), AllowOverdraft: true,
	}
	fxEUR := &domain.Account{
		ID: uuid.Must(uuid.NewV7()), Kind: domain.KindSystem, Code: "fx.EUR",
		Currency: eur, Status: domain.AccountStatus("active"), AllowOverdraft: true,
	}
	repo.putAccount(src)
	repo.putAccount(dst)
	repo.putAccount(fxUSD)
	repo.putAccount(fxEUR)
	hold := &domain.Hold{
		ID: uuid.Must(uuid.NewV7()), AccountID: src.ID, Amount: 400,
		Status: domain.HoldActive, ReferenceID: "tr-1",
		ExpiresAt: time.Unix(500, 0).UTC(),
	}
	repo.putHold(hold)
	svc := New(repo, fakeTx{}, newMemIdem(), &memOutbox{}, fixedClock(time.Unix(100, 0).UTC()), UUIDv7{})

	res, err := svc.CaptureHold(context.Background(), CaptureHoldCmd{
		Idem:          Idem{Key: "svc:transfers/tr-1:capture", RequestHash: []byte("c")},
		HoldID:        hold.ID,
		DestAccountID: dst.ID,
		DestAmount:    money.New(360, eur),
		ReferenceID:   "tr-1",
	})
	require.NoError(t, err)
	require.Equal(t, domain.HoldCaptured, res.Hold.Status)
	require.Len(t, repo.entries, 1)
	postings := repo.entries[0].Postings
	require.Len(t, postings, 4)
	require.Equal(t, src.ID, postings[0].AccountID)
	require.Equal(t, int64(-400), postings[0].Amount)
	require.Equal(t, fxUSD.ID, postings[1].AccountID)
	require.Equal(t, int64(400), postings[1].Amount)
	require.Equal(t, fxEUR.ID, postings[2].AccountID)
	require.Equal(t, int64(-360), postings[2].Amount)
	require.Equal(t, dst.ID, postings[3].AccountID)
	require.Equal(t, int64(360), postings[3].Amount)
}

func mustUSD(t *testing.T) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency("USD")
	require.NoError(t, err)
	return c
}
