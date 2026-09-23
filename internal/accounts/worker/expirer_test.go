package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
