package service

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

// ErrNotImplemented marks use cases that are specified but not implemented yet (see docs/cursor/prompts).
var ErrNotImplemented = errors.New("not implemented")

// Kafka topic for account events. See docs/events.md.
const TopicAccountEvents = "ledger.accounts.v1"

type Service struct {
	repo   Repository
	tx     TxManager
	idem   IdempotencyStore
	outbox OutboxWriter
	clock  Clock
	ids    IDGenerator
}

func New(repo Repository, tx TxManager, idem IdempotencyStore, ob OutboxWriter, clock Clock, ids IDGenerator) *Service {
	return &Service{repo: repo, tx: tx, idem: idem, outbox: ob, clock: clock, ids: ids}
}

// --- Commands. Each mutating command carries the idempotency key and request hash. ---------------

type Idem struct {
	Key         string
	RequestHash []byte
}

type CreateAccountCmd struct {
	Idem     Idem
	OwnerID  uuid.UUID
	Currency money.Currency
}

type DepositCmd struct {
	Idem        Idem
	OwnerID     uuid.UUID
	AccountID   uuid.UUID
	Amount      money.Money
	ExternalRef string
}

type WithdrawCmd = DepositCmd

type CreateHoldCmd struct {
	Idem        Idem
	OwnerID     uuid.UUID // uuid.Nil means an internal caller; owner checks are skipped
	AccountID   uuid.UUID
	Amount      money.Money
	ReferenceID string
	TTL         time.Duration
}

type CaptureHoldCmd struct {
	Idem          Idem
	OwnerID       uuid.UUID // uuid.Nil means an internal caller; owner checks are skipped
	HoldID        uuid.UUID
	DestAccountID uuid.UUID
	DestAmount    money.Money
	ReferenceID   string
	Description   string
}

type ReleaseHoldCmd struct {
	Idem    Idem
	OwnerID uuid.UUID // uuid.Nil means an internal caller; owner checks are skipped
	HoldID  uuid.UUID
	Reason  string
}

type EntryResult struct {
	EntryID uuid.UUID
	Account *domain.Account
}

type CaptureResult struct {
	Hold    *domain.Hold
	EntryID uuid.UUID
}
