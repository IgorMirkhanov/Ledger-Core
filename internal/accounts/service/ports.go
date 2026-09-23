// Package service implements accounts use cases. It depends only on interfaces declared here.
package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/outbox"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

var (
	// ErrHoldReferenceExists is holds_reference_uniq: this account already has a hold with the reference.
	// It aborts the surrounding transaction, so it is a bug signal, not a flow-control path:
	// CreateHold must call FindHold under the account lock before inserting.
	ErrHoldReferenceExists = errors.New("HOLD_REFERENCE_EXISTS")
	// ErrEntryReferenceExists is journal_entries_reference_uniq.
	ErrEntryReferenceExists = errors.New("ENTRY_REFERENCE_EXISTS")
)

// Repository is the persistence port. Every method takes a Querier so it can run inside a transaction.
// Implementation: internal/accounts/repository (pgx, raw SQL).
type Repository interface {
	CreateAccount(ctx context.Context, q postgres.Querier, a *domain.Account) error
	GetAccount(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Account, error)
	ListAccountsByOwner(ctx context.Context, q postgres.Querier, owner uuid.UUID) ([]*domain.Account, error)
	// SystemAccountID returns the id of "<prefix>.<CUR>". Ids are cached: a hit costs no query.
	// Only the id is returned on purpose: balances must be read through LockAccounts, never from an unlocked snapshot.
	SystemAccountID(ctx context.Context, q postgres.Querier, prefix string, cur money.Currency) (uuid.UUID, error)

	// LockAccounts selects accounts FOR UPDATE in ascending id order (deadlock prevention).
	// Returns ErrAccountNotFound if any id is missing.
	LockAccounts(ctx context.Context, q postgres.Querier, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error)
	// UpdateBalances persists balance/held/version of the given (already locked) accounts.
	UpdateBalances(ctx context.Context, q postgres.Querier, accounts ...*domain.Account) error

	// InsertEntry inserts the journal entry and all its postings (with BalanceAfter set).
	InsertEntry(ctx context.Context, q postgres.Querier, e *domain.JournalEntry) error

	CreateHold(ctx context.Context, q postgres.Querier, h *domain.Hold) error
	// FindHold returns the hold of accountID with referenceID, or domain.ErrHoldNotFound.
	// Call it while holding the account lock: that serializes hold creation per account.
	FindHold(ctx context.Context, q postgres.Querier, accountID uuid.UUID, referenceID string) (*domain.Hold, error)
	LockHold(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Hold, error)
	UpdateHold(ctx context.Context, q postgres.Querier, h *domain.Hold) error
	// LockExpiredHolds returns up to limit active holds with expires_at <= now, FOR UPDATE SKIP LOCKED.
	// skip is excluded so a hold that failed alone is not selected again in the same pass.
	LockExpiredHolds(ctx context.Context, q postgres.Querier, now time.Time, limit int, skip []uuid.UUID) ([]*domain.Hold, error)

	// Statement returns postings of an account ordered by id DESC, starting before cursor (0 = newest).
	Statement(ctx context.Context, q postgres.Querier, f StatementFilter) ([]StatementLine, error)
}

type StatementFilter struct {
	AccountID uuid.UUID
	BeforeID  int64 // keyset cursor, 0 = from the newest
	From, To  *time.Time
	Limit     int
}

type StatementLine struct {
	PostingID    int64
	EntryID      uuid.UUID
	Kind         domain.EntryKind
	Amount       int64
	BalanceAfter int64
	Description  string
	CreatedAt    time.Time
}

// TxManager runs fn in a transaction (implemented by *postgres.TxManager).
type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

// IdempotencyStore — implemented by *idempotency.Store.
type IdempotencyStore interface {
	Begin(ctx context.Context, q postgres.Querier, scope, key string, requestHash []byte) (*idempotency.Record, error)
	Complete(ctx context.Context, q postgres.Querier, scope, key string, response []byte, code string) error
}

// OutboxWriter — implemented by *outbox.Writer.
type OutboxWriter interface {
	Add(ctx context.Context, q postgres.Querier, e outbox.Event) error
}

// Clock and IDGenerator make the service deterministic in tests.
type Clock interface{ Now() time.Time }
type IDGenerator interface{ New() uuid.UUID }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type UUIDv7 struct{}

// New returns a time-ordered UUIDv7 (better B-tree locality than v4).
func (UUIDv7) New() uuid.UUID { return uuid.Must(uuid.NewV7()) }
