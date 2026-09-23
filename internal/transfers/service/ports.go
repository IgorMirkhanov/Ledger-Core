// Package service implements the transfer saga orchestrator.
package service

import (
	"context"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
)

type Repository interface {
	Create(ctx context.Context, q postgres.Querier, t *domain.Transfer) error
	Get(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transfer, error)
	// Lock selects the transfer FOR UPDATE.
	Lock(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transfer, error)
	// Update persists the transfer with optimistic check: WHERE id = $1 AND version = t.Version-1.
	Update(ctx context.Context, q postgres.Querier, t *domain.Transfer) error
	ListByOwner(ctx context.Context, q postgres.Querier, owner uuid.UUID, cursor *ListCursor, limit int) ([]*domain.Transfer, error)
	// ClaimPending returns ids of non-terminal transfers with next_attempt_at <= now,
	// using FOR UPDATE SKIP LOCKED so several workers never pick the same transfer.
	ClaimPending(ctx context.Context, q postgres.Querier, now time.Time, limit int) ([]uuid.UUID, error)
	AppendStep(ctx context.Context, q postgres.Querier, s StepLog) error
	GetRate(ctx context.Context, q postgres.Querier, base, quote money.Currency) (*big.Rat, error)
}

type ListCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type StepLog struct {
	TransferID uuid.UUID
	Step       domain.Step
	Outcome    string // ok | business_error | transient_error
	ErrorCode  string
	Error      string
	Duration   time.Duration
}

// AccountsClient is the port to the accounts service (gRPC adapter in transport/accountsclient).
// Errors:
//   - *BusinessError  — definitive refusal (INSUFFICIENT_FUNDS, ACCOUNT_NOT_ACTIVE, HOLD_EXPIRED, ...)
//   - any other error — transient/unknown outcome, the step must be retried with the SAME key.
type AccountsClient interface {
	CreateHold(ctx context.Context, idemKey string, accountID uuid.UUID, amount money.Money, referenceID string, ttl time.Duration) (holdID uuid.UUID, err error)
	CaptureHold(ctx context.Context, idemKey string, holdID, destAccountID uuid.UUID, destAmount money.Money, referenceID, description string) (entryID uuid.UUID, err error)
	ReleaseHold(ctx context.Context, idemKey string, holdID uuid.UUID, reason string) error
	// GetAccountInfo is used to validate ownership/currency before creating a transfer.
	GetAccountInfo(ctx context.Context, ownerID, accountID uuid.UUID) (AccountInfo, error)
}

type AccountInfo struct {
	ID       uuid.UUID
	OwnerID  uuid.UUID
	Currency money.Currency
	Active   bool
}

// BusinessError is a definitive refusal from accounts.
type BusinessError struct {
	Code    string // ErrorInfo.reason from accounts
	Message string
}

func (e *BusinessError) Error() string { return e.Code + ": " + e.Message }

type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type IdempotencyStore interface {
	Begin(ctx context.Context, q postgres.Querier, scope, key string, requestHash []byte) (*idempotency.Record, error)
	Complete(ctx context.Context, q postgres.Querier, scope, key string, response []byte, code string) error
}

type OutboxWriter interface {
	Add(ctx context.Context, q postgres.Querier, e outbox.Event) error
}

type Clock interface{ Now() time.Time }
