// Command gateway is the public REST API: JWT auth, rate limiting, REST → gRPC.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/app"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
)

type Config struct {
	config.Base
	HTTPAddr      string        `env:"HTTP_ADDR" envDefault:":8080"`
	AccountsAddr  string        `env:"ACCOUNTS_GRPC_ADDR,required"`
	TransfersAddr string        `env:"TRANSFERS_GRPC_ADDR,required"`
	RedisAddr     string        `env:"REDIS_ADDR,required"`
	JWTSecret     string        `env:"JWT_SECRET,required"`
	RateLimitRPS  int           `env:"RATE_LIMIT_RPS" envDefault:"50"`
	UpstreamTTL   time.Duration `env:"UPSTREAM_TIMEOUT" envDefault:"5s"`
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

	// TODO(prompt-08): build chi router from internal/gateway with middleware + handlers, gRPC clients, redis.
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteProblem(w, httpx.Problem{
			Type: "about:blank", Title: "Not implemented", Status: http.StatusNotImplemented, Code: "NOT_IMPLEMENTED",
		})
	})

	a := app.New(log, cfg.ShutdownTimeout)
	a.Go(httpx.NewServer("public-api", cfg.HTTPAddr, api, log))
	a.Go(httpx.NewServer("admin", cfg.AdminAddr, httpx.AdminHandler(), log))
	return a.Run(context.Background())
}
