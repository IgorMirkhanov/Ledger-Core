// Package worker runs background maintenance for the accounts service.
package worker

import (
	"context"
	"time"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
)

type holdExpirer interface {
	ExpireHolds(ctx context.Context, batch int) (int, error)
}

// HoldExpirer releases expired holds. A full batch is followed immediately by
// another, so a backlog drains without waiting out the interval.
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
			if err := w.drain(ctx); err != nil {
				return err
			}
			timer.Reset(w.interval)
		}
	}
}

func (w *HoldExpirer) drain(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := w.svc.ExpireHolds(ctx, w.batch)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return err
		}
		if n < w.batch {
			return nil
		}
	}
}
