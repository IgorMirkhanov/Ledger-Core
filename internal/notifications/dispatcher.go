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

const maxSendAttempts = 5

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
	)
	return nil
}

// Dispatcher polls pending notifications and delivers them.
type Dispatcher struct {
	pool     *pgxpool.Pool
	tx       *postgres.TxManager
	sender   Sender
	tpl      *template.Template
	interval time.Duration
	batch    int
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
		log:      log.With(slog.String("component", "notification-dispatcher")),
	}, nil
}

func (d *Dispatcher) Name() string { return "notification-dispatcher" }

func (d *Dispatcher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := d.tick(ctx); err != nil && ctx.Err() == nil {
				metrics.WorkerErrors.WithLabelValues(d.Name()).Inc()
				d.log.Error("dispatcher tick failed", slog.Any("error", err))
			}
			timer.Reset(d.interval)
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) error {
	return d.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, user_id, channel, template, payload, attempts
			FROM notifications
			WHERE status = 'pending'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1`, d.batch)
		if err != nil {
			return fmt.Errorf("claim pending: %w", err)
		}
		defer rows.Close()

		var batch []Notification
		for rows.Next() {
			var n Notification
			if err := rows.Scan(&n.ID, &n.UserID, &n.Channel, &n.Template, &n.Payload, &n.Attempts); err != nil {
				return fmt.Errorf("scan pending: %w", err)
			}
			batch = append(batch, n)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, n := range batch {
			if err := d.deliver(ctx, tx, n); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *Dispatcher) deliver(ctx context.Context, tx pgx.Tx, n Notification) error {
	body, err := d.render(n)
	if err != nil {
		return d.fail(ctx, tx, n, err)
	}
	if err := d.sender.Send(ctx, n, body); err != nil {
		return d.fail(ctx, tx, n, err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE notifications
		SET status = 'sent', attempts = attempts + 1, sent_at = now(), last_error = NULL
		WHERE id = $1`, n.ID)
	return err
}

func (d *Dispatcher) fail(ctx context.Context, tx pgx.Tx, n Notification, cause error) error {
	attempts := n.Attempts + 1
	status := "pending"
	if attempts >= maxSendAttempts {
		status = "failed"
	}
	_, err := tx.Exec(ctx, `
		UPDATE notifications
		SET status = $2::notification_status, attempts = $3, last_error = $4
		WHERE id = $1`,
		n.ID, status, attempts, cause.Error())
	if err != nil {
		return err
	}
	d.log.Warn("notification send failed",
		slog.String("notification_id", n.ID.String()),
		slog.Int("attempts", attempts),
		slog.String("status", status),
		slog.Any("error", cause),
	)
	return nil
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
