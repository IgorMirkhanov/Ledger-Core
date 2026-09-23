// Package app runs a set of long-lived components and shuts them down gracefully.
//
// Shutdown order: on SIGINT/SIGTERM the root context is cancelled, every Runner's Run must return,
// then Closers are called in reverse registration order (e.g. servers → workers → kafka → postgres).
package app

import (
	"context"
	"errors"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Runner is a long-lived component. Run blocks until ctx is cancelled or a fatal error occurs.
// Run must return nil on graceful stop.
type Runner interface {
	Name() string
	Run(ctx context.Context) error
}

// RunnerFunc adapts a function to Runner.
type RunnerFunc struct {
	N string
	F func(ctx context.Context) error
}

func (r RunnerFunc) Name() string                  { return r.N }
func (r RunnerFunc) Run(ctx context.Context) error { return r.F(ctx) }

type closer struct {
	name string
	fn   func(ctx context.Context) error
}

type App struct {
	log             *slog.Logger
	shutdownTimeout time.Duration
	runners         []Runner
	closers         []closer
}

func New(log *slog.Logger, shutdownTimeout time.Duration) *App {
	return &App{log: log, shutdownTimeout: shutdownTimeout}
}

// Go registers a runner.
func (a *App) Go(r Runner) { a.runners = append(a.runners, r) }

// OnShutdown registers a cleanup function; called in reverse order after all runners stopped.
func (a *App) OnShutdown(name string, fn func(ctx context.Context) error) {
	a.closers = append(a.closers, closer{name: name, fn: fn})
}

// Run blocks until a signal arrives or any runner fails, then shuts everything down.
func (a *App) Run(parent context.Context) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	for _, r := range a.runners {
		g.Go(func() error {
			a.log.Info("component started", slog.String("component", r.Name()))
			err := r.Run(gctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.log.Error("component failed", slog.String("component", r.Name()), slog.Any("error", err))
				return err
			}
			a.log.Info("component stopped", slog.String("component", r.Name()))
			return nil
		})
	}

	runErr := g.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()
	var closeErr error
	for i := len(a.closers) - 1; i >= 0; i-- {
		c := a.closers[i]
		if err := c.fn(shutdownCtx); err != nil {
			a.log.Error("shutdown step failed", slog.String("step", c.name), slog.Any("error", err))
			closeErr = errors.Join(closeErr, err)
		}
	}
	a.log.Info("shutdown complete")
	return errors.Join(runErr, closeErr)
}
