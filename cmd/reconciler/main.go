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
	"github.com/IgorMirkhanov/ledger-core/internal/reconciler"
)

type Config struct {
	config.Base
	Postgres       config.Postgres // DSN to the accounts database (writes only reconciliation_*)
	PushgatewayURL string          `env:"PUSHGATEWAY_URL"`
}

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load[Config]()
	if err != nil {
		slog.Error("config", slog.Any("error", err))
		return 1
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		log.Error("postgres", slog.Any("error", err))
		return 1
	}
	defer pool.Close()

	res, err := reconciler.Run(ctx, pool, reconciler.DefaultChecks(), log)
	if err != nil {
		log.Error("reconciler failed", slog.Any("error", err))
		return 1
	}
	if err := reconciler.PushMetrics(ctx, cfg.PushgatewayURL, cfg.ServiceName, res); err != nil {
		log.Warn("pushgateway", slog.Any("error", err))
	}
	if res.Status == "discrepancies" {
		return 2
	}
	return 0
}
