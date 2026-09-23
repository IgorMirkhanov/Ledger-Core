package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

func TestSaga_HappyPathSameCurrency(t *testing.T) {
	rig := newRig(t, false)
	before := testutil.ToFloat64(transfersTotal.WithLabelValues("completed"))
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("same"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, tr.Status)
	require.Equal(t, int64(10_000), tr.DestAmount.Amount())
	require.NotEqual(t, uuid.Nil, tr.JournalEntryID)
	require.Len(t, rig.accounts.entries, 1)
	require.Equal(t, before+1, testutil.ToFloat64(transfersTotal.WithLabelValues("completed")))
	require.Equal(t, []string{"transfer.created", "transfer.completed"}, eventTypes(rig))
}

func TestSaga_HappyPathFX(t *testing.T) {
	rig := newRig(t, true)
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("fx"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, tr.Status)
	require.Equal(t, int64(900_000), tr.DestAmount.Amount())
	require.Equal(t, "RUB", tr.DestAmount.Currency().Code)
	require.Equal(t, int64(900_000), rig.accounts.lastDest.Amount())
	require.NotNil(t, tr.FXRate)
}

func TestSaga_InsufficientFunds(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.holdErr = &BusinessError{Code: domain.FailureInsufficientFunds, Message: "no money"}
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("nsf"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusFailed, tr.Status)
	require.Equal(t, domain.FailureInsufficientFunds, tr.FailureCode)
	require.Equal(t, uuid.Nil, tr.HoldID)
	require.Empty(t, rig.accounts.entries)
	require.Equal(t, "transfer.failed", eventTypes(rig)[len(eventTypes(rig))-1])
}

func TestSaga_DestNotActiveCompensates(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.captureErr = &BusinessError{Code: domain.FailureAccountNotActive, Message: "dest frozen"}
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("frozen"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusFailed, tr.Status)
	require.Equal(t, domain.FailureAccountNotActive, tr.FailureCode)
	require.NotEqual(t, uuid.Nil, tr.HoldID)
	require.Equal(t, 1, rig.accounts.calls["transfer:"+tr.ID.String()+":release"])
}

func TestSaga_CaptureUnavailableThenOK(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.captureFails = 3
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("retry"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusFundsHeld, tr.Status)
	require.Equal(t, 1, tr.Attempts)
	for tr.Status != domain.StatusCompleted {
		tr, err = rig.svc.Advance(context.Background(), tr.ID)
		require.NoError(t, err)
	}
	require.Equal(t, 3, tr.Attempts)
	require.NotEqual(t, uuid.Nil, tr.JournalEntryID)
}

func TestSaga_CaptureTimeoutWasExecuted(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.captureGhost = 1
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("ghost"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusFundsHeld, tr.Status)
	tr, err = rig.svc.Advance(context.Background(), tr.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, tr.Status)
	require.Len(t, rig.accounts.entries, 1)
	require.Equal(t, 2, rig.accounts.calls["transfer:"+tr.ID.String()+":capture"])
}

func TestSaga_ParallelCreateOneTransfer(t *testing.T) {
	rig := newRig(t, false)
	cmd := rig.cmd("parallel")
	var g errgroup.Group
	ids := make([]uuid.UUID, 2)
	for i := range ids {
		g.Go(func() error {
			tr, err := rig.svc.CreateTransfer(context.Background(), cmd)
			if err != nil {
				return err
			}
			ids[i] = tr.ID
			return nil
		})
	}
	require.NoError(t, g.Wait())
	require.Equal(t, ids[0], ids[1])
	require.Len(t, rig.repo.transfers, 1)
}

func TestSaga_SameKeyTwoOwners(t *testing.T) {
	rig := newRig(t, false)
	first := rig.cmd("owners")
	second := rig.cmd("owners")
	second.OwnerID = uuid.Must(uuid.NewV7())
	rig.accounts.infos[rig.dst] = AccountInfo{ID: rig.dst, OwnerID: second.OwnerID, Currency: rig.usd, Active: true}
	rig.accounts.infos[rig.src] = AccountInfo{ID: rig.src, OwnerID: first.OwnerID, Currency: rig.usd, Active: true}
	// source of the second owner is a different account he owns
	src2 := uuid.Must(uuid.NewV7())
	dst2 := uuid.Must(uuid.NewV7())
	rig.accounts.infos[src2] = AccountInfo{ID: src2, OwnerID: second.OwnerID, Currency: rig.usd, Active: true}
	rig.accounts.infos[dst2] = AccountInfo{ID: dst2, OwnerID: first.OwnerID, Currency: rig.usd, Active: true}
	second.SourceID = src2
	second.DestID = dst2
	a, err := rig.svc.CreateTransfer(context.Background(), first)
	require.NoError(t, err)
	b, err := rig.svc.CreateTransfer(context.Background(), second)
	require.NoError(t, err)
	require.NotEqual(t, a.ID, b.ID)
	require.Len(t, rig.repo.transfers, 2)
}

func TestSaga_IdempotencyKeyReused(t *testing.T) {
	rig := newRig(t, false)
	cmd := rig.cmd("reused")
	_, err := rig.svc.CreateTransfer(context.Background(), cmd)
	require.NoError(t, err)
	cmd.RequestHash = []byte("other")
	_, err = rig.svc.CreateTransfer(context.Background(), cmd)
	require.ErrorIs(t, err, idempotency.ErrKeyReused)
	require.Len(t, rig.repo.transfers, 1)
}

func TestSaga_HoldExpired(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.captureErr = &BusinessError{Code: domain.FailureHoldExpired, Message: "expired"}
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("expired"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusFailed, tr.Status)
	require.Equal(t, domain.FailureHoldExpired, tr.FailureCode)
	require.Equal(t, 1, rig.accounts.calls["transfer:"+tr.ID.String()+":release"])
}

func TestSaga_ConcurrentAdvanceOnePosting(t *testing.T) {
	rig := newRig(t, false)
	tr, err := domain.NewTransfer(uuid.Must(uuid.NewV7()), rig.owner, rig.src, rig.dst, money.New(1000, rig.usd), rig.usd, nil, rig.clock.t)
	require.NoError(t, err)
	require.NoError(t, tr.OnHoldCreated(uuid.Must(uuid.NewV7()), rig.clock.t))
	require.NoError(t, rig.repo.Create(context.Background(), nil, tr))

	var n atomic.Int32
	gate := make(chan struct{})
	rig.accounts.beforeCapture = func() {
		if n.Add(1) == 2 {
			close(gate)
		}
		<-gate
	}
	var g errgroup.Group
	for range 2 {
		g.Go(func() error {
			_, err := rig.svc.Advance(context.Background(), tr.ID)
			return err
		})
	}
	require.NoError(t, g.Wait())
	got, err := rig.repo.Get(context.Background(), nil, tr.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompleted, got.Status)
	require.Len(t, rig.accounts.entries, 1)
	require.GreaterOrEqual(t, rig.accounts.calls["transfer:"+tr.ID.String()+":capture"], 1)
}

func TestSaga_ReleaseBugManualReview(t *testing.T) {
	rig := newRig(t, false)
	rig.accounts.captureErr = &BusinessError{Code: domain.FailureAccountNotActive, Message: "frozen"}
	rig.accounts.releaseErr = &BusinessError{Code: "HOLD_NOT_ACTIVE", Message: "already captured"}
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("bug"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusCompensating, tr.Status)
	for tr.Status != domain.StatusFailed {
		tr, err = rig.svc.Advance(context.Background(), tr.ID)
		require.NoError(t, err)
	}
	require.Equal(t, domain.FailureManualReview, tr.FailureCode)
	require.Equal(t, domain.ReleaseAttemptLimit, tr.Attempts)
}

func TestSaga_StopsWhenDeadlineIsNear(t *testing.T) {
	rig := newRig(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	tr, err := rig.svc.CreateTransfer(ctx, rig.cmd("deadline"))
	require.NoError(t, err)
	require.Equal(t, domain.StatusCreated, tr.Status)
	require.Empty(t, rig.accounts.calls)
}

func TestGetTransfer_HidesForeignOwner(t *testing.T) {
	rig := newRig(t, false)
	tr, err := rig.svc.CreateTransfer(context.Background(), rig.cmd("get"))
	require.NoError(t, err)
	_, err = rig.svc.GetTransfer(context.Background(), uuid.Must(uuid.NewV7()), tr.ID)
	require.ErrorIs(t, err, domain.ErrNotFound)
	got, err := rig.svc.GetTransfer(context.Background(), rig.owner, tr.ID)
	require.NoError(t, err)
	require.Equal(t, tr.ID, got.ID)
}

func TestSaga_EmptyDestCurrencyUsesAccount(t *testing.T) {
	rig := newRig(t, false)
	cmd := rig.cmd("empty-dest")
	cmd.DestCurrency = money.Currency{}
	tr, err := rig.svc.CreateTransfer(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, "USD", tr.DestAmount.Currency().Code)
	require.Nil(t, tr.FXRate)

	cmd = rig.cmd("bad-dest")
	cmd.IdemKey = "2"
	cmd.DestCurrency = rig.rub
	_, err = rig.svc.CreateTransfer(context.Background(), cmd)
	require.ErrorIs(t, err, domain.ErrValidation)
}

type rig struct {
	svc      *Service
	repo     *memRepo
	accounts *fakeAccounts
	ob       *memOutbox
	clock    fixedClock
	owner    uuid.UUID
	src      uuid.UUID
	dst      uuid.UUID
	usd      money.Currency
	rub      money.Currency
}

func newRig(t *testing.T, fx bool) *rig {
	t.Helper()
	usd := mustCur(t, "USD")
	rub := mustCur(t, "RUB")
	owner := uuid.Must(uuid.NewV7())
	src := uuid.Must(uuid.NewV7())
	dst := uuid.Must(uuid.NewV7())
	repo := newMemRepo()
	if fx {
		rate, err := money.ParseRate("90")
		require.NoError(t, err)
		repo.rate = rate
	}
	idem := newMemIdem()
	ob := &memOutbox{}
	acc := newFakeAccounts(map[uuid.UUID]AccountInfo{
		src: {ID: src, OwnerID: owner, Currency: usd, Active: true},
		dst: {ID: dst, OwnerID: uuid.Must(uuid.NewV7()), Currency: usd, Active: true},
	})
	if fx {
		acc.infos[dst] = AccountInfo{ID: dst, OwnerID: acc.infos[dst].OwnerID, Currency: rub, Active: true}
	}
	clk := fixedClock{t: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}
	svc := New(repo, &memTx{repo: repo, idem: idem, ob: ob}, idem, ob, acc, clk)
	return &rig{svc: svc, repo: repo, accounts: acc, ob: ob, clock: clk, owner: owner, src: src, dst: dst, usd: usd, rub: rub}
}

func (r *rig) cmd(hash string) CreateTransferCmd {
	dest := r.usd
	if r.repo.rate != nil {
		dest = r.rub
	}
	return CreateTransferCmd{
		IdemKey:      "1",
		RequestHash:  []byte(hash),
		OwnerID:      r.owner,
		SourceID:     r.src,
		DestID:       r.dst,
		Amount:       money.New(10_000, r.usd),
		DestCurrency: dest,
	}
}

func eventTypes(r *rig) []string {
	out := make([]string, len(r.ob.events))
	for i, e := range r.ob.events {
		out[i] = e.EventType
	}
	return out
}

type fakeAccounts struct {
	mu            sync.Mutex
	infos         map[uuid.UUID]AccountInfo
	calls         map[string]int
	holds         map[string]uuid.UUID
	entries       map[string]uuid.UUID
	holdErr       error
	captureErr    error
	captureFails  int
	captureGhost  int
	releaseErr    error
	beforeCapture func()
	lastDest      money.Money
}

func newFakeAccounts(infos map[uuid.UUID]AccountInfo) *fakeAccounts {
	return &fakeAccounts{
		infos:   infos,
		calls:   map[string]int{},
		holds:   map[string]uuid.UUID{},
		entries: map[string]uuid.UUID{},
	}
}

func (f *fakeAccounts) CreateHold(_ context.Context, key string, _ uuid.UUID, _ money.Money, _ string, _ time.Duration) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[key]++
	if id, ok := f.holds[key]; ok {
		return id, nil
	}
	if f.holdErr != nil {
		return uuid.Nil, f.holdErr
	}
	id := uuid.Must(uuid.NewV7())
	f.holds[key] = id
	return id, nil
}

func (f *fakeAccounts) CaptureHold(_ context.Context, key string, _, _ uuid.UUID, dest money.Money, _, _ string) (uuid.UUID, error) {
	if f.beforeCapture != nil {
		f.beforeCapture()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[key]++
	if id, ok := f.entries[key]; ok {
		return id, nil
	}
	if f.captureGhost > 0 {
		f.captureGhost--
		id := uuid.Must(uuid.NewV7())
		f.entries[key] = id
		return uuid.Nil, errors.New("timeout")
	}
	if f.captureFails > 0 {
		f.captureFails--
		return uuid.Nil, errors.New("unavailable")
	}
	if f.captureErr != nil {
		return uuid.Nil, f.captureErr
	}
	id := uuid.Must(uuid.NewV7())
	f.entries[key] = id
	f.lastDest = dest
	return id, nil
}

func (f *fakeAccounts) ReleaseHold(_ context.Context, key string, _ uuid.UUID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[key]++
	return f.releaseErr
}

func (f *fakeAccounts) GetAccountInfo(_ context.Context, owner, id uuid.UUID) (AccountInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, ok := f.infos[id]
	if !ok {
		return AccountInfo{}, &BusinessError{Code: domain.FailureAccountNotFound, Message: "missing"}
	}
	if owner != uuid.Nil && info.OwnerID != owner {
		return AccountInfo{}, &BusinessError{Code: domain.FailureNotAccountOwner, Message: "foreign"}
	}
	return info, nil
}

func mustCur(t *testing.T, code string) money.Currency {
	t.Helper()
	cur, err := money.ParseCurrency(code)
	require.NoError(t, err)
	return cur
}
