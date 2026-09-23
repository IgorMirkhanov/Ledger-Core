// Command accounts runs the ledger service: gRPC API, outbox relay and hold expirer.
package main

import (
	"context"
	"log/slog"
	"os"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/repository"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/transport"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/worker"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
)

type Config struct {
	config.Base
	Postgres config.Postgres
	Kafka    config.Kafka
	GRPC     config.GRPCServer
	Outbox   config.Outbox
}

func main() {
	if err := run(); err != nil {
		slog.Error("accounts exited with error", slog.Any("error", err))
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
		if err := postgres.Migrate(ctx, pool, migrations.Accounts()); err != nil {
			return err
		}
	}

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.ServiceName)
	if err != nil {
		return err
	}

	svc := service.New(repository.New(), postgres.NewTxManager(pool), idempotency.NewStore(),
		outbox.NewWriter(cfg.ServiceName), service.SystemClock{}, service.UUIDv7{})

	grpcSrv := grpcx.NewServer(cfg.GRPC.Addr, log)
	accountsv1.RegisterAccountsServiceServer(grpcSrv.Registrar(), transport.NewHandler(svc))

	admin := httpx.NewServer("admin", cfg.AdminAddr, httpx.AdminHandler(
		httpx.ReadinessCheck{Name: "postgres", Check: pool.Ping},
		httpx.ReadinessCheck{Name: "kafka", Check: producer.Ping},
	), log)

	relay := outbox.NewRelay(pool, producer, outbox.RelayConfig{
		ServiceName:  cfg.ServiceName,
		PollInterval: cfg.Outbox.PollInterval,
		BatchSize:    cfg.Outbox.BatchSize,
	}, log)

	a := app.New(log, cfg.ShutdownTimeout)
	a.Go(grpcSrv)
	a.Go(admin)
	a.Go(relay)
	a.Go(worker.NewHoldExpirer(svc))
	a.Go(worker.NewJanitor(pool, cfg.Outbox.Retention))
	a.OnShutdown("postgres", func(context.Context) error { pool.Close(); return nil })
	a.OnShutdown("kafka", producer.Close)
	return a.Run(ctx)
}
