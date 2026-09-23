package service

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
)

type accountOpened struct {
	AccountID string `json:"account_id"`
	OwnerID   string `json:"owner_id"`
	Currency  string `json:"currency"`
}

type accountMoney struct {
	AccountID    string `json:"account_id"`
	OwnerID      string `json:"owner_id"`
	EntryID      string `json:"entry_id"`
	Amount       int64  `json:"amount"`
	Currency     string `json:"currency"`
	BalanceAfter int64  `json:"balance_after"`
	EntryKind    string `json:"entry_kind"`
}

type holdCreated struct {
	HoldID      string    `json:"hold_id"`
	AccountID   string    `json:"account_id"`
	OwnerID     string    `json:"owner_id"`
	Amount      int64     `json:"amount"`
	Currency    string    `json:"currency"`
	ReferenceID string    `json:"reference_id"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type holdCaptured struct {
	HoldID    string `json:"hold_id"`
	AccountID string `json:"account_id"`
	EntryID   string `json:"entry_id"`
}

type holdReleased struct {
	HoldID    string `json:"hold_id"`
	AccountID string `json:"account_id"`
	Reason    string `json:"reason"`
}

func (s *Service) addEvent(ctx context.Context, tx pgx.Tx, eventType, aggregateType, aggregateID string, payload any) error {
	return s.outbox.Add(ctx, tx, outbox.Event{
		Topic:         TopicAccountEvents,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
	})
}
