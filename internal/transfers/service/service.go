package service

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

var (
	ErrNotImplemented   = errors.New("not implemented")
	ErrConcurrentUpdate = errors.New("CONCURRENT_UPDATE")
)

const TopicTransferEvents = "ledger.transfers.v1"

// HoldTTL must comfortably exceed the worst-case saga duration (all retries).
const HoldTTL = 15 * time.Minute

// StepLease hides an in-flight saga step from other workers. It must exceed the
// accounts call timeout (3s).
const StepLease = 10 * time.Second

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
