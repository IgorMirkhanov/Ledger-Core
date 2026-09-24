package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/metrics"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

const (
	maxSendAttempts = 5
	sendTimeout     = 10 * time.Second
	dispatchLease   = 60 * time.Second
)

// Notification is a pending row ready to send.
type Notification struct {
	ID       uuid.UUID
	UserID   uuid.UUID
	Channel  string
	Template string
	Payload  json.RawMessage
	Attempts int
}

// Sender delivers a rendered notification.
// n.ID is the provider idempotency key: delivery is at-least-once (crash between
// Send and the status UPDATE retries after the lease expires); the provider must
// deduplicate by that key.
type Sender interface {
	Send(ctx context.Context, n Notification, body string) error
}

// LogSender writes deliveries to the application log.
type LogSender struct {
	log *slog.Logger
}

// NewLogSender returns a Sender that logs each delivery.
func NewLogSender(log *slog.Logger) *LogSender {
	if log == nil {
		log = slog.Default()
	}
	return &LogSender{log: log}
}

func (s *LogSender) Send(_ context.Context, n Notification, body string) error {
	s.log.Info("sent email to user",
		slog.String("user_id", n.UserID.String()),
		slog.String("template", n.Template),
		slog.String("channel", n.Channel),
		slog.String("body", body),
		slog.String("notification_id", n.ID.String()),
		slog.String("idempotency_key", n.ID.String()),
	)
	return nil
}

// SendBackoff returns the delay before the next attempt after a failed send.
// attempt is 1-based (the attempt that just failed). Range: 5s … 10m.
func SendBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 5 * time.Second
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= 10*time.Minute {
			return 10 * time.Minute
		}
	}
	return d
}

// Dispatcher polls pending notifications and delivers them outside the claim transaction.
type Dispatcher struct {
	pool     *pgxpool.Pool
	tx       *postgres.TxManager
	sender   Sender
	tpl      *template.Template
	interval time.Duration
	batch    int
	lease    time.Duration
	backoff  func(attempt int) time.Duration
	now      func() time.Time
	log      *slog.Logger
}

// NewDispatcher builds a background dispatcher.
func NewDispatcher(pool *pgxpool.Pool, sender Sender, log *slog.Logger) (*Dispatcher, error) {
	tpl, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{
		pool:     pool,
		tx:       postgres.NewTxManager(pool),
		sender:   sender,
		tpl:      tpl,
		interval: time.Second,
		batch:    50,
		lease:    dispatchLease,
		backoff:  SendBackoff,
		now:      time.Now,
		log:      log.With(slog.String("component", "notification-dispatcher")),
	}, nil
}

func (d *Dispatcher) Name() string { return "notification-dispatcher" }

// ConfigureForTest overrides lease, backoff and clock. For tests only.
func (d *Dispatcher) ConfigureForTest(lease time.Duration, backoff func(int) time.Duration, now func() time.Time) {
	if lease > 0 {
		d.lease = lease
	}
	if backoff != nil {
		d.backoff = backoff
	}
	if now != nil {
		d.now = now
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := d.Tick(ctx); err != nil && ctx.Err() == nil {
				metrics.WorkerErrors.WithLabelValues(d.Name()).Inc()
				d.log.Error("dispatcher tick failed", slog.Any("error", err))
			}
			timer.Reset(d.interval)
		}
	}
}

// Tick claims a batch under a short lease, then sends outside the transaction.
func (d *Dispatcher) Tick(ctx context.Context) error {
	batch, err := d.claim(ctx)
	if err != nil {
		return err
	}
	for _, n := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.deliverOne(ctx, n)
	}
	return nil
}

func (d *Dispatcher) claim(ctx context.Context) ([]Notification, error) {
	now := d.now().UTC()
	leaseUntil := now.Add(d.lease)
	var batch []Notification
	err := d.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE notifications SET next_attempt_at = $2
			WHERE id IN (
				SELECT id FROM notifications
				WHERE status = 'pending' AND next_attempt_at <= $1
				ORDER BY next_attempt_at
				LIMIT $3
				FOR UPDATE SKIP LOCKED)
			RETURNING id, user_id, channel, template, payload, attempts`,
			now, leaseUntil, d.batch)
		if err != nil {
			return fmt.Errorf("claim pending: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var n Notification
			if err := rows.Scan(&n.ID, &n.UserID, &n.Channel, &n.Template, &n.Payload, &n.Attempts); err != nil {
				return fmt.Errorf("scan pending: %w", err)
			}
			batch = append(batch, n)
		}
		return rows.Err()
	})
	return batch, err
}

func (d *Dispatcher) deliverOne(ctx context.Context, n Notification) {
	body, err := d.render(n)
	if err != nil {
		d.markFailed(ctx, n, err)
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if err := d.sender.Send(sendCtx, n, body); err != nil {
		d.markFailed(ctx, n, err)
		return
	}
	if _, err := d.pool.Exec(ctx, `
		UPDATE notifications
		SET status = 'sent', attempts = attempts + 1, sent_at = now(), last_error = NULL
		WHERE id = $1 AND status = 'pending'`, n.ID); err != nil {
		d.log.Error("mark sent failed",
			slog.String("notification_id", n.ID.String()),
			slog.Any("error", err))
	}
}

func (d *Dispatcher) markFailed(ctx context.Context, n Notification, cause error) {
	attempts := n.Attempts + 1
	status := "pending"
	next := d.now().UTC().Add(d.backoff(attempts))
	if attempts >= maxSendAttempts {
		status = "failed"
		next = d.now().UTC() // irrelevant once failed
	}
	_, err := d.pool.Exec(ctx, `
		UPDATE notifications
		SET status = $2::notification_status,
		    attempts = $3,
		    last_error = $4,
		    next_attempt_at = $5
		WHERE id = $1 AND status = 'pending'`,
		n.ID, status, attempts, cause.Error(), next)
	if err != nil {
		d.log.Error("mark failed update error",
			slog.String("notification_id", n.ID.String()),
			slog.Any("error", err))
		return
	}
	d.log.Warn("notification send failed",
		slog.String("notification_id", n.ID.String()),
		slog.Int("attempts", attempts),
		slog.String("status", status),
		slog.Time("next_attempt_at", next),
		slog.Any("error", cause),
	)
}

func (d *Dispatcher) render(n Notification) (string, error) {
	var data map[string]any
	if err := json.Unmarshal(n.Payload, &data); err != nil {
		return "", fmt.Errorf("payload: %w", err)
	}
	data["user_id"] = n.UserID.String()
	var buf bytes.Buffer
	if err := d.tpl.ExecuteTemplate(&buf, n.Template, data); err != nil {
		return "", fmt.Errorf("template %s: %w", n.Template, err)
	}
	return buf.String(), nil
}
