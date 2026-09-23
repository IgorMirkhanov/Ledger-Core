// Command notifications consumes ledger events and stores/sends user notifications (inbox pattern).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
)

type Config struct {
	config.Base
	Postgres      config.Postgres
	Kafka         config.Kafka
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP" envDefault:"notifications"`
	MaxAttempts   int    `env:"CONSUMER_MAX_ATTEMPTS" envDefault:"5"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("notifications exited with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load[Config]()
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)
	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	if cfg.Postgres.MigrateOnStart {
		if err := postgres.Migrate(ctx, pool, migrations.Notifications()); err != nil {
			return err
		}
	}

	a := app.New(log, cfg.ShutdownTimeout)
	a.Go(httpx.NewServer("admin", cfg.AdminAddr, httpx.AdminHandler(
		httpx.ReadinessCheck{Name: "postgres", Check: pool.Ping},
	), log))
	// TODO(prompt-09): a.Go(consumer) — internal/notifications.
	a.OnShutdown("postgres", func(context.Context) error { pool.Close(); return nil })
	return a.Run(ctx)
}
