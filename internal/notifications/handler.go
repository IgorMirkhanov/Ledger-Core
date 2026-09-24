package notifications

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/kafka"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Handler consumes ledger.* topics and writes inbox + notifications atomically.
type Handler struct {
	pool *pgxpool.Pool
	tx   *postgres.TxManager
}

// NewHandler builds a Kafka handler backed by the notifications database.
func NewHandler(pool *pgxpool.Pool) *Handler {
	return &Handler{pool: pool, tx: postgres.NewTxManager(pool)}
}

// Handle implements kafka.Handler.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	return h.tx.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO processed_events (event_id, event_type, topic, partition, "offset")
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (event_id) DO NOTHING`,
			msg.Envelope.EventID, msg.Envelope.EventType, msg.Topic, msg.Partition, msg.Offset)
		if err != nil {
			return fmt.Errorf("notifications: claim event: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // duplicate delivery
		}

		d, err := MapEvent(msg.Envelope)
		if err != nil {
			return errors.Join(kafka.ErrPermanent, err)
		}
		if d == nil {
			return nil
		}
		id := uuid.Must(uuid.NewV7())
		_, err = tx.Exec(ctx, `
			INSERT INTO notifications (id, user_id, event_id, channel, template, payload, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending')`,
			id, d.UserID, msg.Envelope.EventID, d.Channel, d.Template, d.Payload)
		if err != nil {
			return fmt.Errorf("notifications: insert: %w", err)
		}
		return nil
	})
}
