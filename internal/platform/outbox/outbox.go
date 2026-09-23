// Package outbox implements the transactional outbox pattern.
//
// Writer.Add is called inside the business transaction. Relay runs in the background,
// reads unpublished rows and publishes them to Kafka (at-least-once).
// Consumers deduplicate by Envelope.EventID (inbox pattern). See docs/events.md.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Event is what business code writes into the outbox.
type Event struct {
	Topic         string
	AggregateType string // "account", "transfer"
	AggregateID   string // Kafka message key → per-aggregate ordering
	EventType     string // "account.credited", "transfer.completed", ...
	Payload       any    // marshaled to JSON as Envelope.Data
	Headers       map[string]string
}

// Envelope is the wire format of every Kafka message value (JSON).
type Envelope struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	Data          json.RawMessage `json:"data"`
}

// Writer inserts events into the outbox table.
type Writer struct {
	producer string
	now      func() time.Time
}

func NewWriter(producer string) *Writer {
	return &Writer{producer: producer, now: time.Now}
}

// Add must be called with the transaction of the business operation.
func (w *Writer) Add(ctx context.Context, q postgres.Querier, e Event) error {
	data, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}
	env := Envelope{
		EventID:       uuid.New(),
		EventType:     e.EventType,
		SchemaVersion: 1,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		OccurredAt:    w.now().UTC(),
		Producer:      w.producer,
		Data:          data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	headers := e.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	// TODO(prompt-03): inject W3C traceparent from ctx into headers (otel propagator).
	hdr, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("outbox: marshal headers: %w", err)
	}
	_, err = q.Exec(ctx, `
		INSERT INTO outbox (event_id, topic, aggregate_type, aggregate_id, event_type, payload, headers)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		env.EventID, e.Topic, e.AggregateType, e.AggregateID, e.EventType, payload, hdr)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// DeletePublished removes up to limit rows published before olderThan.
func DeletePublished(ctx context.Context, q postgres.Querier, olderThan time.Time, limit int) (int64, error) {
	tag, err := q.Exec(ctx, `
		DELETE FROM outbox
		WHERE ctid IN (
			SELECT ctid FROM outbox
			WHERE published_at IS NOT NULL AND published_at < $1
			LIMIT $2
		)`, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("outbox: cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}
