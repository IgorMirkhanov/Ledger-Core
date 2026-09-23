// Package worker retries transfers whose next attempt is due.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

type advancer interface {
	ClaimPending(ctx context.Context, limit int) ([]uuid.UUID, error)
	Advance(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
}

// Recovery claims due transfers and advances them with bounded parallelism.
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
			return ctx.Err()
		case <-timer.C:
			if err := w.tick(ctx); err != nil {
				return err
			}
			timer.Reset(w.interval)
		}
	}
}

func (w *Recovery) tick(ctx context.Context) error {
	ids, err := w.svc.ClaimPending(ctx, w.limit)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	}
	sem := make(chan struct{}, w.parallel)
	var wg sync.WaitGroup
	for _, id := range ids {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(id uuid.UUID) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := w.svc.Advance(ctx, id); err != nil && ctx.Err() == nil {
				slog.Error("recovery advance", slog.String("transfer_id", id.String()), slog.Any("error", err))
			}
		}(id)
	}
	wg.Wait()
	return nil
}
