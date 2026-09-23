//go:build integration

// Package testenv starts shared Postgres and Redpanda containers for integration tests.
package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/config"
	pg "github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// One container per test process. Each test gets its own database so cases do not leak rows.
var (
	pgOnce      sync.Once
	pgErr       error
	pgContainer *postgres.PostgresContainer
	adminPool   *pgxpool.Pool
	adminDSN    string
)

// SetupPostgres starts the shared Postgres container. TestMain calls it once;
// StartPostgres also calls it, so a single test still works.
func SetupPostgres(ctx context.Context) error {
	pgOnce.Do(func() { pgErr = startPostgres(ctx) })
	return pgErr
}

func startPostgres(ctx context.Context) error {
	ctr, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return fmt.Errorf("testenv: start postgres: %w", err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = ctr.Terminate(context.WithoutCancel(ctx))
		return fmt.Errorf("testenv: postgres dsn: %w", err)
	}
	pool, err := pg.NewPool(ctx, config.Postgres{
		DSN:             dsn,
		MaxConns:        4,
		MinConns:        0,
		MaxConnLifetime: time.Hour,
	})
	if err != nil {
		_ = ctr.Terminate(context.WithoutCancel(ctx))
		return fmt.Errorf("testenv: admin pool: %w", err)
	}
	pgContainer = ctr
	adminPool = pool
	adminDSN = dsn
	return nil
}

// StartPostgres returns a pool for a fresh empty database. The database is dropped on cleanup.
func StartPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	require.NoError(t, SetupPostgres(ctx))

	name, err := randomDBName()
	require.NoError(t, err)
	ident := pgx.Identifier{name}.Sanitize()

	createCtx, createCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer createCancel()
	// CREATE DATABASE cannot run inside a transaction.
	_, err = adminPool.Exec(createCtx, "CREATE DATABASE "+ident)
	require.NoError(t, err)

	dsn, err := dsnWithDatabase(adminDSN, name)
	require.NoError(t, err)
	pool, err := pg.NewPool(createCtx, config.Postgres{
		DSN:             dsn,
		MaxConns:        32,
		MinConns:        0,
		MaxConnLifetime: time.Hour,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dropCancel()
		rows, qerr := adminPool.Query(dropCtx,
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
			name)
		if qerr == nil {
			rows.Close()
		}
		if _, derr := adminPool.Exec(dropCtx, "DROP DATABASE IF EXISTS "+ident); derr != nil {
			t.Errorf("drop database %s: %v", name, derr)
		}
	})
	return pool
}

// MigratedPool is StartPostgres plus goose migrations from fsys.
func MigratedPool(t *testing.T, fsys fs.FS) *pgxpool.Pool {
	t.Helper()
	pool := StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, pg.Migrate(ctx, pool, fsys))
	return pool
}

// Teardown stops containers started by this process. Call from TestMain after m.Run.
func Teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if pgContainer != nil {
		_ = pgContainer.Terminate(ctx)
		pgContainer = nil
	}
	teardownRedpanda(ctx)
}

func randomDBName() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("testenv: random db name: %w", err)
	}
	return "t_" + hex.EncodeToString(b[:]), nil
}

func dsnWithDatabase(raw, name string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("testenv: parse dsn: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}
