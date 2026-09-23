package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

func TestRecovery_AdvancesClaimedWithBoundedParallelism(t *testing.T) {
	ids := make([]uuid.UUID, 20)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	fake := &fakeSaga{ids: ids}
	w := &Recovery{svc: fake, interval: time.Hour, limit: 50, parallel: 10}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return fake.advanced.Load() == int32(len(ids)) }, 3*time.Second, 10*time.Millisecond)
	require.LessOrEqual(t, int(fake.max.Load()), 10)
	cancel()
	require.NoError(t, <-done)
}

type fakeSaga struct {
	ids      []uuid.UUID
	once     atomic.Bool
	advanced atomic.Int32
	inflight atomic.Int32
	max      atomic.Int32
}

func (f *fakeSaga) ClaimPending(context.Context, int) ([]uuid.UUID, error) {
	if f.once.Swap(true) {
		return nil, nil
	}
	return f.ids, nil
}

func (f *fakeSaga) Advance(context.Context, uuid.UUID) (*domain.Transfer, error) {
	n := f.inflight.Add(1)
	for {
		cur := f.max.Load()
		if n <= cur || f.max.CompareAndSwap(cur, n) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	f.inflight.Add(-1)
	f.advanced.Add(1)
	return &domain.Transfer{}, nil
}

func TestRecovery_LogsClaimErrorAndContinues(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	fake := &flakySaga{id: id, claimErr: errors.New("db down")}
	w := &Recovery{svc: fake, interval: 15 * time.Millisecond, limit: 50, parallel: 10}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool { return fake.advanced.Load() == 1 && fake.claims.Load() >= 2 }, 2*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

type flakySaga struct {
	id       uuid.UUID
	claimErr error
	claims   atomic.Int32
	advanced atomic.Int32
}

func (f *flakySaga) ClaimPending(context.Context, int) ([]uuid.UUID, error) {
	if f.claims.Add(1) == 1 {
		return nil, f.claimErr
	}
	return []uuid.UUID{f.id}, nil
}

func (f *flakySaga) Advance(context.Context, uuid.UUID) (*domain.Transfer, error) {
	f.advanced.Add(1)
	return &domain.Transfer{}, nil
}
