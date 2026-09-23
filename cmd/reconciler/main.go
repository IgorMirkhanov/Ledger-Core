// Command reconciler verifies ledger invariants (G1, G2, G3, G9) and exits with code 2 on discrepancies.
//
// Run once:   reconciler
// Run as a job in docker-compose / k8s CronJob.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

type Config struct {
	config.Base
	Postgres config.Postgres // read-only DSN to the accounts database
}

func main() {
	cfg, err := config.Load[Config]()
	if err != nil {
		slog.Error("config", slog.Any("error", err))
		os.Exit(1)
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		log.Error("postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	// TODO(prompt-10): internal/reconciler.Run(ctx, pool) → report; os.Exit(2) if discrepancies.
	log.Info("reconciler is not implemented yet")
}
