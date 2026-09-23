package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJanitor_ErrorThenNextTick(t *testing.T) {
	var calls atomic.Int32
	j := &Janitor{
		interval: 15 * time.Millisecond,
		sweep: func(context.Context) error {
			if calls.Add(1) == 1 {
				return errors.New("db down")
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.Run(ctx) }()

	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 2*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}
