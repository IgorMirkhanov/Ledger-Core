package service

import (
	"context"
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
	AccountID   uuid.UUID
	Amount      money.Money
	ReferenceID string
	TTL         time.Duration
}

type CaptureHoldCmd struct {
	Idem          Idem
	HoldID        uuid.UUID
	DestAccountID uuid.UUID
	DestAmount    money.Money
	ReferenceID   string
	Description   string
}

type ReleaseHoldCmd struct {
	Idem   Idem
	HoldID uuid.UUID
	Reason string
}

type EntryResult struct {
	EntryID uuid.UUID
	Account *domain.Account
}

type CaptureResult struct {
	Hold    *domain.Hold
	EntryID uuid.UUID
}

// Use cases. Specs: docs/cursor/prompts/05-accounts-service-grpc.md.

func (s *Service) CreateAccount(ctx context.Context, cmd CreateAccountCmd) (*domain.Account, error) {
	return nil, ErrNotImplemented
}

func (s *Service) GetAccount(ctx context.Context, owner, id uuid.UUID) (*domain.Account, error) {
	return nil, ErrNotImplemented
}

func (s *Service) ListAccounts(ctx context.Context, owner uuid.UUID) ([]*domain.Account, error) {
	return nil, ErrNotImplemented
}

func (s *Service) Deposit(ctx context.Context, cmd DepositCmd) (*EntryResult, error) {
	return nil, ErrNotImplemented
}

func (s *Service) Withdraw(ctx context.Context, cmd WithdrawCmd) (*EntryResult, error) {
	return nil, ErrNotImplemented
}

func (s *Service) CreateHold(ctx context.Context, cmd CreateHoldCmd) (*domain.Hold, error) {
	return nil, ErrNotImplemented
}

func (s *Service) CaptureHold(ctx context.Context, cmd CaptureHoldCmd) (*CaptureResult, error) {
	return nil, ErrNotImplemented
}

func (s *Service) ReleaseHold(ctx context.Context, cmd ReleaseHoldCmd) (*domain.Hold, error) {
	return nil, ErrNotImplemented
}

func (s *Service) Statement(ctx context.Context, owner uuid.UUID, f StatementFilter) ([]StatementLine, error) {
	return nil, ErrNotImplemented
}

// ExpireHolds releases expired holds in batches. Called periodically by a worker.
func (s *Service) ExpireHolds(ctx context.Context, batch int) (int, error) {
	return 0, ErrNotImplemented
}
