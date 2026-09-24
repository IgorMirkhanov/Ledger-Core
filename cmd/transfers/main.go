// Command transfers runs the transfer saga orchestrator: gRPC API, recovery worker and outbox relay.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/accountsclient"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/repository"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/transport"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/worker"
	"github.com/IgorMirkhanov/ledger-core/migrations"
)

type Config struct {
	config.Base
	Postgres     config.Postgres
	Kafka        config.Kafka
	GRPC         config.GRPCServer
	Outbox       config.Outbox
	AccountsAddr string        `env:"ACCOUNTS_GRPC_ADDR,required"`
	RecoveryTick time.Duration `env:"RECOVERY_INTERVAL" envDefault:"1s"`
}

type clock struct{}

func (clock) Now() time.Time { return time.Now().UTC() }

func main() {
	if err := run(); err != nil {
		slog.Error("transfers exited with error", slog.Any("error", err))
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

	if err := config.CheckProd(cfg.Base, config.CommonProdProblems(cfg.Postgres, cfg.Kafka, &cfg.GRPC)...); err != nil {
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
		if err := postgres.Migrate(ctx, pool, migrations.Transfers()); err != nil {
			return err
		}
	}
	if cfg.Postgres.MigrateOnly {
		log.Info("migrations applied, exiting (MIGRATE_ONLY)")
		pool.Close()
		return shutdownTrace(ctx)
	}
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.ServiceName, kafka.WithAutoCreateTopics(cfg.Kafka.AutoCreateTopics))
	if err != nil {
		return err
	}

	accounts, err := accountsclient.New(cfg.AccountsAddr)
	if err != nil {
		return err
	}
	svc := service.New(repository.New(), postgres.NewTxManager(pool), idempotency.NewStore(),
		outbox.NewWriter(cfg.ServiceName), accounts, clock{})

	grpcSrv := grpcx.NewServer(cfg.GRPC.Addr, log)
	if cfg.GRPC.Reflection {
		grpcSrv.EnableReflection()
	}
	transfersv1.RegisterTransfersServiceServer(grpcSrv.Registrar(), transport.NewHandler(svc))

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
	a.Go(worker.NewRecovery(svc, cfg.RecoveryTick))
	a.OnShutdown("accounts-client", func(context.Context) error { return accounts.Close() })
	a.OnShutdown("postgres", func(context.Context) error { pool.Close(); return nil })
	a.OnShutdown("kafka", producer.Close)
	a.OnShutdown("otel", shutdownTrace)
	return a.Run(ctx)
}
