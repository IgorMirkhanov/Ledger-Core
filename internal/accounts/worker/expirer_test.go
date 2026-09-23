package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeExpirer struct {
	calls atomic.Int32
}

func (f *fakeExpirer) ExpireHolds(context.Context, int) (int, error) {
	if f.calls.Add(1) == 1 {
		return 100, nil
	}
	return 1, nil
}

func (f *fakeExpirer) ExpireNextHold(context.Context, []uuid.UUID) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func TestHoldExpirer_DrainsFullBatchWithoutWaiting(t *testing.T) {
	fake := &fakeExpirer{}
	w := &HoldExpirer{svc: fake, interval: time.Hour, batch: 100}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for fake.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expirer did not stop")
	}
	if fake.calls.Load() < 2 {
		t.Fatalf("full batch was not followed immediately, calls=%d", fake.calls.Load())
	}
}

func TestHoldExpirer_ErrorThenNextTick(t *testing.T) {
	fake := &countingExpirer{err: errors.New("db down")}
	w := &HoldExpirer{svc: fake, interval: 15 * time.Millisecond, batch: 100}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for fake.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expirer did not stop")
	}
	if fake.calls.Load() < 2 {
		t.Fatalf("second tick did not run, calls=%d", fake.calls.Load())
	}
}

type countingExpirer struct {
	err   error
	calls atomic.Int32
}

func (c *countingExpirer) ExpireHolds(context.Context, int) (int, error) {
	if c.calls.Add(1) == 1 {
		return 0, c.err
	}
	return 0, nil
}

func (c *countingExpirer) ExpireNextHold(context.Context, []uuid.UUID) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func TestHoldExpirer_PoisonHoldDoesNotBlockTheOthers(t *testing.T) {
	good1 := uuid.Must(uuid.NewV7())
	bad := uuid.Must(uuid.NewV7())
	good2 := uuid.Must(uuid.NewV7())
	fake := &poisonExpirer{
		batchErr: errors.New("unreserve"),
		ones: []nextHold{
			{id: bad, err: errors.New("held underflow")},
			{id: good1},
			{id: good2},
		},
		expired: map[uuid.UUID]bool{},
	}
	w := &HoldExpirer{svc: fake, interval: time.Hour, batch: 3}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for !fake.finished(good1, good2) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expirer did not stop")
	}
	if !fake.finished(good1, good2) || fake.wasExpired(bad) || fake.missed() {
		t.Fatalf("expired = %v missedSkip=%v", fake.snapshot(), fake.missed())
	}
}

type nextHold struct {
	id  uuid.UUID
	err error
}

type poisonExpirer struct {
	batchErr   error
	ones       []nextHold
	mu         sync.Mutex
	calls      int
	expired    map[uuid.UUID]bool
	missedSkip bool
}

func (p *poisonExpirer) ExpireHolds(context.Context, int) (int, error) {
	return 0, p.batchErr
}

func (p *poisonExpirer) ExpireNextHold(_ context.Context, skip []uuid.UUID) (uuid.UUID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls >= len(p.ones) {
		return uuid.Nil, nil
	}
	item := p.ones[p.calls]
	p.calls++
	if p.calls > 1 {
		seen := false
		for _, id := range skip {
			if id == p.ones[0].id {
				seen = true
			}
		}
		if !seen {
			p.missedSkip = true
		}
	}
	if item.err != nil {
		return item.id, item.err
	}
	p.expired[item.id] = true
	return item.id, nil
}

func (p *poisonExpirer) finished(a, b uuid.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expired[a] && p.expired[b]
}

func (p *poisonExpirer) wasExpired(id uuid.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expired[id]
}

func (p *poisonExpirer) missed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.missedSkip
}

func (p *poisonExpirer) snapshot() map[uuid.UUID]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[uuid.UUID]bool, len(p.expired))
	for id, ok := range p.expired {
		out[id] = ok
	}
	return out
}
