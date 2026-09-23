package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Janitor deletes expired idempotency keys and published outbox rows past retention.
type Janitor struct {
	q         postgres.Querier
	idem      *idempotency.Store
	retention time.Duration
	interval  time.Duration
	batch     int
	sweep     func(context.Context) error
}

func NewJanitor(q postgres.Querier, retention time.Duration) *Janitor {
	if retention <= 0 {
		retention = 168 * time.Hour
	}
	j := &Janitor{
		q:         q,
		idem:      idempotency.NewStore(),
		retention: retention,
		interval:  10 * time.Minute,
		batch:     1000,
	}
	j.sweep = j.Sweep
	return j
}

func (j *Janitor) Name() string { return "janitor" }

func (j *Janitor) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			fn := j.sweep
			if fn == nil {
				fn = j.Sweep
			}
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				workerErrors.WithLabelValues(j.Name()).Inc()
				slog.Error("janitor sweep", slog.String("component", j.Name()), slog.Any("error", err))
			}
			timer.Reset(j.interval)
		}
	}
}

// Sweep runs one cleanup pass. It stops between batches when ctx is cancelled.
func (j *Janitor) Sweep(ctx context.Context) error {
	if err := j.untilShort(ctx, func(ctx context.Context) (int64, error) {
		return j.idem.DeleteExpired(ctx, j.q, j.batch)
	}); err != nil {
		return err
	}
	cutoff := time.Now().Add(-j.retention)
	return j.untilShort(ctx, func(ctx context.Context) (int64, error) {
		return outbox.DeletePublished(ctx, j.q, cutoff, j.batch)
	})
}

func (j *Janitor) untilShort(ctx context.Context, fn func(context.Context) (int64, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := fn(ctx)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return err
		}
		if n < int64(j.batch) {
			return nil
		}
	}
}
