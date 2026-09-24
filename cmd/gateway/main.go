package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	accountsv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/accounts/v1"
	transfersv1 "github.com/IgorMirkhanov/ledger-core/gen/ledger/transfers/v1"
	"github.com/IgorMirkhanov/ledger-core/internal/gateway"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
)

type Config struct {
	config.Base
	HTTPAddr          string        `env:"HTTP_ADDR" envDefault:":8080"`
	AccountsAddr      string        `env:"ACCOUNTS_GRPC_ADDR,required"`
	TransfersAddr     string        `env:"TRANSFERS_GRPC_ADDR,required"`
	RedisAddr         string        `env:"REDIS_ADDR,required"`
	JWTSecret         string        `env:"JWT_SECRET,required"`
	RateLimitRPS      int           `env:"RATE_LIMIT_RPS" envDefault:"50"`
	RateLimitIPRPS    int           `env:"RATE_LIMIT_IP_RPS" envDefault:"100"`
	TrustedProxyCIDRs string        `env:"TRUSTED_PROXY_CIDRS"`
	UpstreamTTL       time.Duration `env:"UPSTREAM_TIMEOUT" envDefault:"5s"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("gateway exited with error", slog.Any("error", err))
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
	shutdownTrace, err := observability.InitTracing(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}

	accountsConn, err := grpcx.Dial(cfg.AccountsAddr)
	if err != nil {
		return err
	}
	transfersConn, err := grpcx.Dial(cfg.TransfersAddr)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	trusted, err := gateway.ParseTrustedProxies(cfg.TrustedProxyCIDRs)
	if err != nil {
		return fmt.Errorf("TRUSTED_PROXY_CIDRS: %w", err)
	}

	api := &gateway.API{
		Accounts:  accountsv1.NewAccountsServiceClient(accountsConn),
		Transfers: transfersv1.NewTransfersServiceClient(transfersConn),
		JWTSecret: []byte(cfg.JWTSecret),
		Upstream:  cfg.UpstreamTTL,
		AppEnv:    cfg.Env,
	}
	router := gateway.NewRouter(gateway.Options{
		API:            api,
		Redis:          rdb,
		RateLimitRPS:   cfg.RateLimitRPS,
		RateLimitIP:    cfg.RateLimitIPRPS,
		TrustedProxies: trusted,
		JWTSecret:      []byte(cfg.JWTSecret),
		AppEnv:         cfg.Env,
		UpstreamTTL:    cfg.UpstreamTTL,
	})

	accountsHealth := healthpb.NewHealthClient(accountsConn)
	transfersHealth := healthpb.NewHealthClient(transfersConn)
	admin := httpx.AdminHandler(
		httpx.ReadinessCheck{Name: "redis", Check: gateway.PingRedis(rdb)},
		httpx.ReadinessCheck{Name: "accounts", Check: grpcReady(accountsHealth)},
		httpx.ReadinessCheck{Name: "transfers", Check: grpcReady(transfersHealth)},
	)

	a := app.New(log, cfg.ShutdownTimeout)
	a.Go(httpx.NewServer("public-api", cfg.HTTPAddr, router, log))
	a.Go(httpx.NewServer("admin", cfg.AdminAddr, admin, log))
	a.OnShutdown("accounts-grpc", func(context.Context) error { return accountsConn.Close() })
	a.OnShutdown("transfers-grpc", func(context.Context) error { return transfersConn.Close() })
	a.OnShutdown("redis", func(ctx context.Context) error { return rdb.Close() })
	a.OnShutdown("otel", shutdownTrace)
	return a.Run(context.Background())
}

func grpcReady(c healthpb.HealthClient) func(context.Context) error {
	return func(ctx context.Context) error {
		resp, err := c.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving: %s", resp.GetStatus())
		}
		return nil
	}
}
