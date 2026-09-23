// Package config loads service configuration from environment variables.
//
// Every service embeds Base and adds its own fields. Env names are documented in .env.example.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Base is shared by all services.
type Base struct {
	ServiceName     string        `env:"SERVICE_NAME,required"`
	Env             string        `env:"APP_ENV" envDefault:"local"` // local | test | prod
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	AdminAddr       string        `env:"ADMIN_ADDR" envDefault:":8081"` // /healthz /readyz /metrics
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
	OTLPEndpoint    string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"` // empty disables tracing export
}

type Postgres struct {
	DSN             string        `env:"POSTGRES_DSN,required"`
	MaxConns        int32         `env:"POSTGRES_MAX_CONNS" envDefault:"20"`
	MinConns        int32         `env:"POSTGRES_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime time.Duration `env:"POSTGRES_MAX_CONN_LIFETIME" envDefault:"30m"`
	MigrateOnStart  bool          `env:"POSTGRES_MIGRATE_ON_START" envDefault:"true"`
}

type Kafka struct {
	Brokers  []string `env:"KAFKA_BROKERS,required" envSeparator:","`
	ClientID string   `env:"KAFKA_CLIENT_ID"`
}

type GRPCServer struct {
	Addr string `env:"GRPC_ADDR" envDefault:":9090"`
}

type Outbox struct {
	PollInterval time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"200ms"`
	BatchSize    int           `env:"OUTBOX_BATCH_SIZE" envDefault:"100"`
	Retention    time.Duration `env:"OUTBOX_RETENTION" envDefault:"168h"`
}

// Load parses env into cfg (a pointer to a struct embedding Base).
func Load[T any]() (*T, error) {
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &cfg, nil
}
