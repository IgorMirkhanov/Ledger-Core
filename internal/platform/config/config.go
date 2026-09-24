// Package config loads service configuration from environment variables.
//
// Every service embeds Base and adds its own fields. Env names are documented in .env.example.
package config

import (
	"errors"
	"fmt"
	"strings"
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

// IsProd reports APP_ENV=prod. Production refuses unsafe settings at startup (see CheckProd).
func (b Base) IsProd() bool { return b.Env == "prod" }

type Postgres struct {
	DSN             string        `env:"POSTGRES_DSN,required"`
	MaxConns        int32         `env:"POSTGRES_MAX_CONNS" envDefault:"20"`
	MinConns        int32         `env:"POSTGRES_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime time.Duration `env:"POSTGRES_MAX_CONN_LIFETIME" envDefault:"30m"`
	MigrateOnStart  bool          `env:"POSTGRES_MIGRATE_ON_START" envDefault:"true"`
	// MigrateOnly applies migrations and exits. Used by the migration Job in Kubernetes.
	MigrateOnly bool `env:"MIGRATE_ONLY" envDefault:"false"`
}

type Kafka struct {
	Brokers  []string `env:"KAFKA_BROKERS,required" envSeparator:","`
	ClientID string   `env:"KAFKA_CLIENT_ID"`
	// AutoCreateTopics lets the producer create missing topics with broker defaults.
	// Convenient locally; in production topics are created explicitly (partitions, RF=3).
	AutoCreateTopics bool `env:"KAFKA_AUTO_CREATE_TOPICS" envDefault:"true"`
}

type GRPCServer struct {
	Addr string `env:"GRPC_ADDR" envDefault:":9090"`
	// Reflection exposes the full API description to anyone who reaches the port. Local tooling only.
	Reflection bool `env:"GRPC_REFLECTION" envDefault:"true"`
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

// Unsafe returns msg when cond holds, "" otherwise. Helper for CheckProd.
func Unsafe(cond bool, msg string) string {
	if cond {
		return msg
	}
	return ""
}

// CheckProd fails startup in production when any unsafe setting is present.
// Non-production environments are never blocked.
func CheckProd(b Base, problems ...string) error {
	if !b.IsProd() {
		return nil
	}
	var found []string
	for _, p := range problems {
		if p != "" {
			found = append(found, p)
		}
	}
	if len(found) == 0 {
		return nil
	}
	return errors.New("config: unsafe settings for APP_ENV=prod: " + strings.Join(found, "; "))
}

// CommonProdProblems are checks shared by services that own a database, Kafka and gRPC.
func CommonProdProblems(pg Postgres, k Kafka, g *GRPCServer) []string {
	out := []string{
		Unsafe(pg.MigrateOnStart && !pg.MigrateOnly, "POSTGRES_MIGRATE_ON_START must be false (run the migration Job instead)"),
		Unsafe(k.AutoCreateTopics, "KAFKA_AUTO_CREATE_TOPICS must be false (create topics explicitly)"),
	}
	if g != nil {
		out = append(out, Unsafe(g.Reflection, "GRPC_REFLECTION must be false"))
	}
	return out
}
