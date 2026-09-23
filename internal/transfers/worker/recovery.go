// Package worker retries transfers whose next attempt is due.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/metrics"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

type advancer interface {
	ClaimPending(ctx context.Context, limit int) ([]uuid.UUID, error)
	Advance(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
}

// Recovery claims due transfers and advances them with bounded parallelism.
// An iteration error is logged; Run returns only when ctx is cancelled.
type Recovery struct {
	svc      advancer
	interval time.Duration
	limit    int
	parallel int
}

func NewRecovery(svc *service.Service, interval time.Duration) *Recovery {
	if interval <= 0 {
		interval = time.Second
	}
	return &Recovery{svc: svc, interval: interval, limit: 50, parallel: 10}
}

func (w *Recovery) Name() string { return "transfer-recovery" }

func (w *Recovery) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			w.tick(ctx)
			timer.Reset(w.interval)
		}
	}
}

func (w *Recovery) tick(ctx context.Context) {
	ids, err := w.svc.ClaimPending(ctx, w.limit)
	if err != nil {
		if ctx.Err() == nil {
			w.note(uuid.Nil, err)
		}
		return
	}
	sem := make(chan struct{}, w.parallel)
	var wg sync.WaitGroup
	for _, id := range ids {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(id uuid.UUID) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := w.svc.Advance(ctx, id); err != nil && ctx.Err() == nil {
				w.note(id, err)
			}
		}(id)
	}
	wg.Wait()
}

func (w *Recovery) note(id uuid.UUID, err error) {
	metrics.WorkerErrors.WithLabelValues(w.Name()).Inc()
	args := []any{slog.String("component", w.Name()), slog.Any("error", err)}
	if id != uuid.Nil {
		args = append(args, slog.String("transfer_id", id.String()))
	}
	slog.Error("recovery", args...)
}
