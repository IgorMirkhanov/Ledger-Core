// Package repository is the pgx implementation of the transfers saga store.
// Optimistic updates and ClaimPending follow docs/database.md §2.
package repository

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/transfers/service"
)

// Repository has no connection of its own: every method takes a postgres.Querier.
type Repository struct{}

func New() *Repository { return &Repository{} }

var _ service.Repository = (*Repository)(nil)

const transferCols = `
	id, owner_id, source_account_id, dest_account_id,
	amount, currency, dest_amount, dest_currency, fx_rate::text,
	status, hold_id, journal_entry_id, failure_code, failure_reason,
	attempts, next_attempt_at, version, created_at, updated_at, completed_at`

func (r *Repository) Create(ctx context.Context, q postgres.Querier, t *domain.Transfer) error {
	_, err := q.Exec(ctx, `
		INSERT INTO transfers (
			id, owner_id, source_account_id, dest_account_id,
			amount, currency, dest_amount, dest_currency, fx_rate,
			status, hold_id, journal_entry_id, failure_code, failure_reason,
			attempts, next_attempt_at, version, created_at, updated_at, completed_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10::transfer_status, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20
		)`,
		t.ID, t.OwnerID, t.SourceAccountID, t.DestAccountID,
		t.Amount.Amount(), t.Amount.Currency().Code, t.DestAmount.Amount(), t.DestAmount.Currency().Code, ratString(t.FXRate),
		string(t.Status), nullUUID(t.HoldID), nullUUID(t.JournalEntryID), nullString(t.FailureCode), nullString(t.FailureReason),
		t.Attempts, t.NextAttemptAt, t.Version, t.CreatedAt, t.UpdatedAt, t.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("transfers: create: %w", err)
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transfer, error) {
	t, err := scanTransfer(q.QueryRow(ctx, `SELECT `+transferCols+` FROM transfers WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("transfers: get: %w", err)
	}
	return t, nil
}

func (r *Repository) Lock(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Transfer, error) {
	t, err := scanTransfer(q.QueryRow(ctx, `SELECT `+transferCols+` FROM transfers WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("transfers: lock: %w", err)
	}
	return t, nil
}

func (r *Repository) Update(ctx context.Context, q postgres.Querier, t *domain.Transfer) error {
	tag, err := q.Exec(ctx, `
		UPDATE transfers SET
			status = $3::transfer_status,
			hold_id = $4,
			journal_entry_id = $5,
			failure_code = $6,
			failure_reason = $7,
			attempts = $8,
			next_attempt_at = $9,
			version = $10,
			updated_at = $11,
			completed_at = $12
		WHERE id = $1 AND version = $2`,
		t.ID, t.Version-1,
		string(t.Status), nullUUID(t.HoldID), nullUUID(t.JournalEntryID),
		nullString(t.FailureCode), nullString(t.FailureReason),
		t.Attempts, t.NextAttemptAt, t.Version, t.UpdatedAt, t.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("transfers: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return service.ErrConcurrentUpdate
	}
	return nil
}

func (r *Repository) ListByOwner(ctx context.Context, q postgres.Querier, owner uuid.UUID, cursor *service.ListCursor, limit int) ([]*domain.Transfer, error) {
	var createdAt *time.Time
	var id *uuid.UUID
	if cursor != nil {
		createdAt = &cursor.CreatedAt
		id = &cursor.ID
	}
	rows, err := q.Query(ctx, `
		SELECT `+transferCols+`
		FROM transfers
		WHERE owner_id = $1
		  AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`, owner, createdAt, id, limit)
	if err != nil {
		return nil, fmt.Errorf("transfers: list: %w", err)
	}
	defer rows.Close()
	var out []*domain.Transfer
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			return nil, fmt.Errorf("transfers: list: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("transfers: list: %w", err)
	}
	return out, nil
}

func (r *Repository) ClaimPending(ctx context.Context, q postgres.Querier, now, lease time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx, `
		UPDATE transfers SET next_attempt_at = $2
		WHERE id IN (
			SELECT id FROM transfers
			WHERE status IN ('created', 'funds_held', 'compensating')
			  AND next_attempt_at <= $1
			ORDER BY next_attempt_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		RETURNING id`, now, lease, limit)
	if err != nil {
		return nil, fmt.Errorf("transfers: claim pending: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("transfers: claim pending: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("transfers: claim pending: %w", err)
	}
	return ids, nil
}

func (r *Repository) Lease(ctx context.Context, q postgres.Querier, id uuid.UUID, until time.Time) error {
	tag, err := q.Exec(ctx, `UPDATE transfers SET next_attempt_at = $2 WHERE id = $1`, id, until)
	if err != nil {
		return fmt.Errorf("transfers: lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r *Repository) AppendStep(ctx context.Context, q postgres.Querier, s service.StepLog) error {
	_, err := q.Exec(ctx, `
		INSERT INTO transfer_steps (transfer_id, step, outcome, error_code, error, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		s.TransferID, string(s.Step), s.Outcome, nullString(s.ErrorCode), nullString(s.Error), s.Duration.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf("transfers: append step: %w", err)
	}
	return nil
}

func (r *Repository) GetRate(ctx context.Context, q postgres.Querier, base, quote money.Currency) (*big.Rat, error) {
	var raw string
	err := q.QueryRow(ctx, `SELECT rate::text FROM fx_rates WHERE base = $1 AND quote = $2`, base.Code, quote.Code).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrFXRateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("transfers: get rate: %w", err)
	}
	rate, ok := new(big.Rat).SetString(raw)
	if !ok || rate.Sign() <= 0 {
		return nil, fmt.Errorf("transfers: get rate: %w", money.ErrInvalidRate)
	}
	return rate, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanTransfer(row scannable) (*domain.Transfer, error) {
	var (
		t            domain.Transfer
		amount       int64
		currency     string
		destAmount   int64
		destCurrency string
		fx           *string
		status       string
		holdID       *uuid.UUID
		entryID      *uuid.UUID
		failureCode  *string
		failureWhy   *string
	)
	err := row.Scan(
		&t.ID, &t.OwnerID, &t.SourceAccountID, &t.DestAccountID,
		&amount, &currency, &destAmount, &destCurrency, &fx,
		&status, &holdID, &entryID, &failureCode, &failureWhy,
		&t.Attempts, &t.NextAttemptAt, &t.Version, &t.CreatedAt, &t.UpdatedAt, &t.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	cur, err := money.ParseCurrency(strings.TrimSpace(currency))
	if err != nil {
		return nil, fmt.Errorf("transfers: currency: %w", err)
	}
	dst, err := money.ParseCurrency(strings.TrimSpace(destCurrency))
	if err != nil {
		return nil, fmt.Errorf("transfers: dest currency: %w", err)
	}
	t.Amount = money.New(amount, cur)
	t.DestAmount = money.New(destAmount, dst)
	t.Status = domain.Status(status)
	if fx != nil {
		rate, ok := new(big.Rat).SetString(*fx)
		if !ok {
			return nil, fmt.Errorf("transfers: fx rate %q: %w", *fx, money.ErrInvalidRate)
		}
		t.FXRate = rate
	}
	if holdID != nil {
		t.HoldID = *holdID
	}
	if entryID != nil {
		t.JournalEntryID = *entryID
	}
	if failureCode != nil {
		t.FailureCode = *failureCode
	}
	if failureWhy != nil {
		t.FailureReason = *failureWhy
	}
	return &t, nil
}

func ratString(r *big.Rat) any {
	if r == nil {
		return nil
	}
	return r.FloatString(10)
}

func nullUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
