package service

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

var (
	ErrNotImplemented   = errors.New("not implemented")
	ErrConcurrentUpdate = errors.New("CONCURRENT_UPDATE")
)

const TopicTransferEvents = "ledger.transfers.v1"

// HoldTTL must comfortably exceed the worst-case saga duration (all retries).
const HoldTTL = 15 * time.Minute

type Service struct {
	repo     Repository
	tx       TxManager
	idem     IdempotencyStore
	outbox   OutboxWriter
	accounts AccountsClient
	clock    Clock
}

func New(repo Repository, tx TxManager, idem IdempotencyStore, ob OutboxWriter, accounts AccountsClient, clock Clock) *Service {
	return &Service{repo: repo, tx: tx, idem: idem, outbox: ob, accounts: accounts, clock: clock}
}

type CreateTransferCmd struct {
	IdemKey      string
	RequestHash  []byte
	OwnerID      uuid.UUID
	SourceID     uuid.UUID
	DestID       uuid.UUID
	Amount       money.Money
	DestCurrency money.Currency
	Description  string
}

// Specs: docs/saga.md and docs/cursor/prompts/06-transfers-saga.md.

// CreateTransfer persists the transfer (idempotently) and then drives it synchronously via Advance
// until it is terminal or the ctx deadline is near. Returns the latest state.
func (s *Service) CreateTransfer(ctx context.Context, cmd CreateTransferCmd) (*domain.Transfer, error) {
	return nil, ErrNotImplemented
}

// Advance executes the next saga step of one transfer (one step per call, loop until terminal).
// Safe to call concurrently for the same id: the transfer row is locked while its state changes.
func (s *Service) Advance(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	return nil, ErrNotImplemented
}

func (s *Service) GetTransfer(ctx context.Context, owner, id uuid.UUID) (*domain.Transfer, error) {
	return nil, ErrNotImplemented
}

func (s *Service) ListTransfers(ctx context.Context, owner uuid.UUID, cursor *ListCursor, limit int) ([]*domain.Transfer, *ListCursor, error) {
	return nil, nil, ErrNotImplemented
}

// Backoff returns exponential backoff with full jitter: rand[0, min(cap, base*2^attempt)).
func Backoff(attempt int) time.Duration {
	const (
		base = 200 * time.Millisecond
		cap_ = 30 * time.Second
	)
	d := base << min(attempt, 10)
	if d > cap_ || d <= 0 {
		d = cap_
	}
	return time.Duration(rand.Int64N(int64(d))) //nolint:gosec // jitter does not need crypto randomness
}
