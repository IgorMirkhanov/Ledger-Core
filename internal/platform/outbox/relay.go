package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Message is a record ready to be published.
type Message struct {
	ID      int64
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Publisher publishes a batch synchronously (acks=all). It must return an error if ANY message failed.
// Implemented by internal/platform/kafka.Producer.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

type RelayConfig struct {
	ServiceName  string
	PollInterval time.Duration
	BatchSize    int
}

// Relay moves rows from the outbox table to Kafka.
//
// Ordering: exactly one relay per service is active, guaranteed by a Postgres advisory lock
// held on a dedicated connection. Other replicas stay in standby and retry the lock.
// Delivery: at-least-once. A crash between Publish and UPDATE republishes the batch.
type Relay struct {
	pool *pgxpool.Pool
	pub  Publisher
	cfg  RelayConfig
	log  *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, pub Publisher, cfg RelayConfig, log *slog.Logger) *Relay {
	return &Relay{pool: pool, pub: pub, cfg: cfg, log: log.With(slog.String("component", "outbox-relay"))}
}

func (r *Relay) Name() string { return "outbox-relay" }

// Run blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := r.runAsLeader(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("relay stopped, will retry", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}

func (r *Relay) lockKey() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("outbox-relay:" + r.cfg.ServiceName))
	return int64(h.Sum64())
}

func (r *Relay) runAsLeader(ctx context.Context) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, r.lockKey()).Scan(&locked); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	if !locked {
		return nil // another replica is the leader
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, r.lockKey())
	}()
	r.log.Info("became outbox leader")

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	pendingEvery := time.NewTicker(5 * time.Second)
	defer pendingEvery.Stop()
	for {
		n, err := r.publishBatch(ctx)
		if err != nil {
			return err
		}
		if n == r.cfg.BatchSize {
			continue // backlog: drain without waiting
		}
		select {
		case <-ctx.Done():
			return nil
		case <-pendingEvery.C:
			r.refreshPending(ctx)
		case <-ticker.C:
		}
	}
}

func (r *Relay) refreshPending(ctx context.Context) {
	var n int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		return
	}
	observability.OutboxPending.WithLabelValues(r.cfg.ServiceName).Set(float64(n))
}

// publishBatch publishes one batch inside a transaction and marks rows as published.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	var (
		published int
		failedIDs []int64
	)
	err := postgres.NewTxManager(r.pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, topic, aggregate_id, payload, headers
			FROM outbox
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("select: %w", err)
		}
		msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Message, error) {
			var (
				m       Message
				key     string
				headers []byte
			)
			if err := row.Scan(&m.ID, &m.Topic, &key, &m.Value, &headers); err != nil {
				return m, err
			}
			m.Key = []byte(key)
			if err := json.Unmarshal(headers, &m.Headers); err != nil {
				return m, err
			}
			return m, nil
		})
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if len(msgs) == 0 {
			return nil
		}

		ids := make([]int64, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		if err := r.pub.Publish(ctx, msgs); err != nil {
			failedIDs = ids
			return fmt.Errorf("publish: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return fmt.Errorf("mark published: %w", err)
		}
		published = len(msgs)
		return nil
	})
	if len(failedIDs) > 0 {
		// Only after rollback: the rows are no longer locked by our own transaction.
		r.recordFailure(context.WithoutCancel(ctx), failedIDs, err)
		observability.OutboxPublishErrors.WithLabelValues(r.cfg.ServiceName).Add(float64(len(failedIDs)))
	}
	if published > 0 {
		observability.OutboxPublished.WithLabelValues(r.cfg.ServiceName).Add(float64(published))
	}
	return published, err
}

func (r *Relay) recordFailure(ctx context.Context, ids []int64, cause error) {
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = ANY($1)`, ids, cause.Error())
	if err != nil {
		r.log.Error("failed to record outbox failure", slog.Any("error", err))
	}
}
