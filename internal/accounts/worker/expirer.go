// Package worker runs background maintenance for the accounts service.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/metrics"
)

type holdExpirer interface {
	ExpireHolds(ctx context.Context, batch int) (int, error)
	// ExpireNextHold expires one due hold, ignoring skip. The id is set even when that hold fails.
	ExpireNextHold(ctx context.Context, skip []uuid.UUID) (uuid.UUID, error)
}

// HoldExpirer releases expired holds. A full batch is followed immediately by
// another, so a backlog drains without waiting out the interval.
// A database error is logged; Run returns only when ctx is cancelled.
type HoldExpirer struct {
	svc      holdExpirer
	interval time.Duration
	batch    int
}

func NewHoldExpirer(svc *service.Service) *HoldExpirer {
	return &HoldExpirer{svc: svc, interval: 5 * time.Second, batch: 100}
}

func (w *HoldExpirer) Name() string { return "hold-expirer" }

func (w *HoldExpirer) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			w.drain(ctx)
			timer.Reset(w.interval)
		}
	}
}

func (w *HoldExpirer) drain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := w.svc.ExpireHolds(ctx, w.batch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.note(uuid.Nil, err)
			w.expireOneByOne(ctx)
			return
		}
		if n < w.batch {
			return
		}
	}
}

// expireOneByOne retries the failed batch hold by hold. A hold that fails is skipped
// for the rest of this pass so the others can still expire.
func (w *HoldExpirer) expireOneByOne(ctx context.Context) {
	var skip []uuid.UUID
	for range w.batch {
		if ctx.Err() != nil {
			return
		}
		id, err := w.svc.ExpireNextHold(ctx, skip)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.note(id, err)
			if id == uuid.Nil {
				return
			}
			skip = append(skip, id)
			continue
		}
		if id == uuid.Nil {
			return
		}
	}
}

func (w *HoldExpirer) note(id uuid.UUID, err error) {
	metrics.WorkerErrors.WithLabelValues(w.Name()).Inc()
	args := []any{slog.String("component", w.Name()), slog.Any("error", err)}
	if id != uuid.Nil {
		args = append(args, slog.String("hold_id", id.String()))
	}
	slog.Error("hold expirer", args...)
}
