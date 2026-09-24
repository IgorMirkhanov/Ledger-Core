// Command notifications consumes ledger events and stores/sends user notifications (inbox pattern).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/IgorMirkhanov/ledger-core/internal/notifications"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
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

	if err := config.CheckProd(cfg.Base, config.CommonProdProblems(cfg.Postgres, cfg.Kafka, nil)...); err != nil {
		return err
	}

	shutdownTrace, err := observability.InitTracing(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	if cfg.Postgres.MigrateOnStart || cfg.Postgres.MigrateOnly {
		if err := postgres.Migrate(ctx, pool, migrations.Notifications()); err != nil {
			return err
		}
	}
	if cfg.Postgres.MigrateOnly {
		log.Info("migrations applied, exiting (MIGRATE_ONLY)")
		pool.Close()
		return shutdownTrace(ctx)
	}

	// Producer is used only for readiness ping (consumer has no separate Ping).
	pinger, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.ServiceName+"-ready", kafka.WithAutoCreateTopics(cfg.Kafka.AutoCreateTopics))
	if err != nil {
		return err
	}

	handler := notifications.NewHandler(pool)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     cfg.Kafka.Brokers,
		Group:       cfg.ConsumerGroup,
		Topics:      []string{"ledger.accounts.v1", "ledger.transfers.v1"},
		MaxAttempts: cfg.MaxAttempts,
		DLQTopic:    "ledger.notifications.dlq",
	}, handler.Handle, log)
	if err != nil {
		return err
	}

	dispatcher, err := notifications.NewDispatcher(pool, notifications.NewLogSender(log), log)
	if err != nil {
		return err
	}

	a := app.New(log, cfg.ShutdownTimeout)
	a.Go(httpx.NewServer("admin", cfg.AdminAddr, httpx.AdminHandler(
		httpx.ReadinessCheck{Name: "postgres", Check: pool.Ping},
		httpx.ReadinessCheck{Name: "kafka", Check: pinger.Ping, Optional: true},
	), log))
	a.Go(consumer)
	a.Go(dispatcher)
	a.OnShutdown("kafka-ready", pinger.Close)
	a.OnShutdown("postgres", func(context.Context) error { pool.Close(); return nil })
	a.OnShutdown("otel", shutdownTrace)
	return a.Run(ctx)
}
