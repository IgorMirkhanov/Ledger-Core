// Package repository is the pgx implementation of service.Repository.
// Queries that move money follow docs/database.md §1.3.
package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/accounts/domain"
	"github.com/IgorMirkhanov/ledger-core/internal/accounts/service"
	"github.com/IgorMirkhanov/ledger-core/internal/money"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Repository has no connection of its own: every method takes a postgres.Querier.
// systemIDs caches ids of system accounts; their codes never change.
type Repository struct {
	systemIDs sync.Map
}

func New() *Repository { return &Repository{} }

var _ service.Repository = (*Repository)(nil)

const accountCols = `id, kind, owner_id, code, currency, status, balance, held, allow_overdraft, version, created_at, updated_at`

func (r *Repository) CreateAccount(ctx context.Context, q postgres.Querier, a *domain.Account) error {
	_, err := q.Exec(ctx, `
		INSERT INTO accounts (
			id, kind, owner_id, code, currency, status, balance, held, allow_overdraft, version, created_at, updated_at
		) VALUES (
			$1, $2::account_kind, $3, $4, $5, $6::account_status, $7, $8, $9, $10, $11, $12
		)`,
		a.ID, string(a.Kind), nullUUID(a.OwnerID), nullString(a.Code), a.Currency.Code,
		string(a.Status), a.Balance, a.Held, a.AllowOverdraft, a.Version, a.CreatedAt, a.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("accounts: create account: %w", err)
	}
	return nil
}

func (r *Repository) GetAccount(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Account, error) {
	a, err := scanAccount(q.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("accounts: get account: %w", err)
	}
	return a, nil
}

func (r *Repository) ListAccountsByOwner(ctx context.Context, q postgres.Querier, owner uuid.UUID) ([]*domain.Account, error) {
	rows, err := q.Query(ctx, `
		SELECT `+accountCols+`
		FROM accounts
		WHERE owner_id = $1
		ORDER BY created_at, id`, owner)
	if err != nil {
		return nil, fmt.Errorf("accounts: list accounts: %w", err)
	}
	defer rows.Close()
	var out []*domain.Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("accounts: list accounts: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accounts: list accounts: %w", err)
	}
	return out, nil
}

func (r *Repository) SystemAccountID(ctx context.Context, q postgres.Querier, prefix string, cur money.Currency) (uuid.UUID, error) {
	code := domain.SystemCode(prefix, cur)
	if cached, ok := r.systemIDs.Load(code); ok {
		return cached.(uuid.UUID), nil
	}
	var id uuid.UUID
	err := q.QueryRow(ctx, `SELECT id FROM accounts WHERE code = $1 AND kind = 'system'`, code).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, domain.ErrAccountNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("accounts: system account id: %w", err)
	}
	r.systemIDs.Store(code, id)
	return id, nil
}

func (r *Repository) LockAccounts(ctx context.Context, q postgres.Querier, ids []uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	ids = uniqueUUIDs(ids)
	if len(ids) == 0 {
		return map[uuid.UUID]*domain.Account{}, nil
	}
	rows, err := q.Query(ctx, `
		SELECT `+accountCols+`
		FROM accounts
		WHERE id = ANY($1::uuid[])
		ORDER BY id
		FOR UPDATE`, ids)
	if err != nil {
		return nil, fmt.Errorf("accounts: lock accounts: %w", err)
	}
	defer rows.Close()
	out := make(map[uuid.UUID]*domain.Account, len(ids))
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("accounts: lock accounts: %w", err)
		}
		out[a.ID] = a
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accounts: lock accounts: %w", err)
	}
	if len(out) != len(ids) {
		return nil, domain.ErrAccountNotFound
	}
	return out, nil
}

func (r *Repository) UpdateBalances(ctx context.Context, q postgres.Querier, accounts ...*domain.Account) error {
	accounts = dedupeAccounts(accounts)
	if len(accounts) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, len(accounts))
	balances := make([]int64, len(accounts))
	held := make([]int64, len(accounts))
	versions := make([]int64, len(accounts))
	updated := make([]time.Time, len(accounts))
	for i, a := range accounts {
		ids[i] = a.ID
		balances[i] = a.Balance
		held[i] = a.Held
		versions[i] = a.Version
		updated[i] = a.UpdatedAt
	}
	tag, err := q.Exec(ctx, `
		UPDATE accounts AS a
		SET balance = v.balance, held = v.held, version = v.version, updated_at = v.updated_at
		FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::bigint[]) AS balance,
		             unnest($3::bigint[]) AS held, unnest($4::bigint[]) AS version,
		             unnest($5::timestamptz[]) AS updated_at) AS v
		WHERE a.id = v.id`, ids, balances, held, versions, updated)
	if err != nil {
		return fmt.Errorf("accounts: update balances: %w", err)
	}
	if tag.RowsAffected() != int64(len(accounts)) {
		return fmt.Errorf("accounts: update balances: updated %d of %d", tag.RowsAffected(), len(accounts))
	}
	return nil
}

func (r *Repository) InsertEntry(ctx context.Context, q postgres.Querier, e *domain.JournalEntry) error {
	if e == nil || !e.IsApplied() {
		return domain.ErrEntryNotApplied
	}
	_, err := q.Exec(ctx, `
		INSERT INTO journal_entries (id, kind, reference_type, reference_id, description, created_at)
		VALUES ($1, $2::journal_entry_kind, $3, $4, $5, $6)`,
		e.ID, string(e.Kind), e.ReferenceType, e.ReferenceID, e.Description, e.CreatedAt,
	)
	if err != nil {
		if mapped := mapConstraint(err); mapped != nil {
			return mapped
		}
		return fmt.Errorf("accounts: insert entry: %w", err)
	}
	n := len(e.Postings)
	accountIDs := make([]uuid.UUID, n)
	amounts := make([]int64, n)
	currencies := make([]string, n)
	balances := make([]int64, n)
	for i, p := range e.Postings {
		accountIDs[i] = p.AccountID
		amounts[i] = p.Amount
		currencies[i] = p.Currency.Code
		balances[i] = p.BalanceAfter
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
		SELECT $1, unnest($2::uuid[]), unnest($3::bigint[]), unnest($4::char(3)[]), unnest($5::bigint[])`,
		e.ID, accountIDs, amounts, currencies, balances); err != nil {
		return fmt.Errorf("accounts: insert postings: %w", err)
	}
	return nil
}

func (r *Repository) CreateHold(ctx context.Context, q postgres.Querier, h *domain.Hold) error {
	_, err := q.Exec(ctx, `
		INSERT INTO holds (
			id, account_id, amount, status, reference_id, expires_at, journal_entry_id, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4::hold_status, $5, $6, $7, $8, $9
		)`,
		h.ID, h.AccountID, h.Amount, string(h.Status), h.ReferenceID, h.ExpiresAt,
		nullUUID(h.JournalEntryID), h.CreatedAt, h.UpdatedAt,
	)
	if err != nil {
		if mapped := mapConstraint(err); mapped != nil {
			return mapped
		}
		return fmt.Errorf("accounts: create hold: %w", err)
	}
	return nil
}

func (r *Repository) FindHold(ctx context.Context, q postgres.Querier, accountID uuid.UUID, referenceID string) (*domain.Hold, error) {
	h, err := scanHold(q.QueryRow(ctx, `
		SELECT id, account_id, amount, status, reference_id, expires_at, journal_entry_id, created_at, updated_at
		FROM holds
		WHERE account_id = $1 AND reference_id = $2`, accountID, referenceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrHoldNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("accounts: find hold: %w", err)
	}
	return h, nil
}

func (r *Repository) LockHold(ctx context.Context, q postgres.Querier, id uuid.UUID) (*domain.Hold, error) {
	h, err := scanHold(q.QueryRow(ctx, `
		SELECT id, account_id, amount, status, reference_id, expires_at, journal_entry_id, created_at, updated_at
		FROM holds
		WHERE id = $1
		FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrHoldNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("accounts: lock hold: %w", err)
	}
	return h, nil
}

func (r *Repository) UpdateHold(ctx context.Context, q postgres.Querier, h *domain.Hold) error {
	tag, err := q.Exec(ctx, `
		UPDATE holds
		SET status = $2::hold_status, journal_entry_id = $3, updated_at = $4
		WHERE id = $1`,
		h.ID, string(h.Status), nullUUID(h.JournalEntryID), h.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("accounts: update hold: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrHoldNotFound
	}
	return nil
}

func (r *Repository) LockExpiredHolds(ctx context.Context, q postgres.Querier, now time.Time, limit int, skip []uuid.UUID) ([]*domain.Hold, error) {
	if skip == nil {
		skip = []uuid.UUID{}
	}
	rows, err := q.Query(ctx, `
		SELECT id, account_id, amount, status, reference_id, expires_at, journal_entry_id, created_at, updated_at
		FROM holds
		WHERE status = 'active' AND expires_at <= $1
		  AND NOT (id = ANY($3::uuid[]))
		ORDER BY expires_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, now, limit, skip)
	if err != nil {
		return nil, fmt.Errorf("accounts: lock expired holds: %w", err)
	}
	defer rows.Close()
	var out []*domain.Hold
	for rows.Next() {
		h, err := scanHold(rows)
		if err != nil {
			return nil, fmt.Errorf("accounts: lock expired holds: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accounts: lock expired holds: %w", err)
	}
	return out, nil
}

func (r *Repository) Statement(ctx context.Context, q postgres.Querier, f service.StatementFilter) ([]service.StatementLine, error) {
	var from, to any
	if f.From != nil {
		from = *f.From
	}
	if f.To != nil {
		to = *f.To
	}
	rows, err := q.Query(ctx, `
		SELECT p.id, p.entry_id, e.kind, p.amount, p.balance_after, e.description, p.created_at
		FROM postings p
		JOIN journal_entries e ON e.id = p.entry_id
		WHERE p.account_id = $1
		  AND ($2::bigint = 0 OR p.id < $2)
		  AND ($3::timestamptz IS NULL OR p.created_at >= $3)
		  AND ($4::timestamptz IS NULL OR p.created_at <  $4)
		ORDER BY p.id DESC
		LIMIT $5`, f.AccountID, f.BeforeID, from, to, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("accounts: statement: %w", err)
	}
	defer rows.Close()
	var out []service.StatementLine
	for rows.Next() {
		var (
			line service.StatementLine
			kind string
		)
		if err := rows.Scan(&line.PostingID, &line.EntryID, &kind, &line.Amount, &line.BalanceAfter, &line.Description, &line.CreatedAt); err != nil {
			return nil, fmt.Errorf("accounts: statement: %w", err)
		}
		line.Kind = domain.EntryKind(kind)
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("accounts: statement: %w", err)
	}
	return out, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanAccount(row scannable) (*domain.Account, error) {
	var (
		a      domain.Account
		kind   string
		owner  *uuid.UUID
		code   *string
		cur    string
		status string
	)
	err := row.Scan(
		&a.ID, &kind, &owner, &code, &cur, &status,
		&a.Balance, &a.Held, &a.AllowOverdraft, &a.Version, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	parsed, err := money.ParseCurrency(strings.TrimSpace(cur))
	if err != nil {
		return nil, fmt.Errorf("accounts: currency: %w", err)
	}
	a.Kind = domain.AccountKind(kind)
	a.Status = domain.AccountStatus(status)
	a.Currency = parsed
	if owner != nil {
		a.OwnerID = *owner
	}
	if code != nil {
		a.Code = *code
	}
	return &a, nil
}

func scanHold(row scannable) (*domain.Hold, error) {
	var (
		h       domain.Hold
		status  string
		entryID *uuid.UUID
	)
	err := row.Scan(
		&h.ID, &h.AccountID, &h.Amount, &status, &h.ReferenceID,
		&h.ExpiresAt, &entryID, &h.CreatedAt, &h.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	h.Status = domain.HoldStatus(status)
	if entryID != nil {
		h.JournalEntryID = *entryID
	}
	return &h, nil
}

func mapConstraint(err error) error {
	switch postgres.ConstraintName(err) {
	case "holds_reference_uniq":
		return service.ErrHoldReferenceExists
	case "journal_entries_reference_uniq":
		return service.ErrEntryReferenceExists
	default:
		return nil
	}
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

func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func dedupeAccounts(accounts []*domain.Account) []*domain.Account {
	idx := make(map[uuid.UUID]int, len(accounts))
	out := make([]*domain.Account, 0, len(accounts))
	for _, a := range accounts {
		if a == nil {
			continue
		}
		if i, ok := idx[a.ID]; ok {
			out[i] = a
			continue
		}
		idx[a.ID] = len(out)
		out = append(out, a)
	}
	return out
}
