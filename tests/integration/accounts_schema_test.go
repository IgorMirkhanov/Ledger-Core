//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
	"github.com/IgorMirkhanov/ledger-core/migrations"
	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestSchema_BalancedEntryInserts(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "USD")
	system := systemAccount(t, ctx, pool, "settlement.USD")

	entryID := uuid.Must(uuid.NewV7())
	err := postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertPostings(ctx, tx, entryID, customer, system, 10000, -10000, "USD", "USD")
	})
	require.NoError(t, err)

	var sum int64
	err = pool.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0) FROM postings WHERE entry_id = $1`, entryID).Scan(&sum)
	require.NoError(t, err)
	require.Equal(t, int64(0), sum)
}

func TestSchema_UnbalancedEntryRejected(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "USD")
	system := systemAccount(t, ctx, pool, "settlement.USD")

	entryID := uuid.Must(uuid.NewV7())
	err := postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertPostings(ctx, tx, entryID, customer, system, 10000, -4000, "USD", "USD")
	})
	requireCheckViolation(t, err)
}

func TestSchema_SinglePostingRejected(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "USD")

	entryID := uuid.Must(uuid.NewV7())
	err := postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := insertJournal(ctx, tx, entryID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
			VALUES ($1, $2, 10000, 'USD', 10000)`, entryID, customer)
		return err
	})
	requireCheckViolation(t, err)
}

func TestSchema_CurrencyMismatchRejected(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "USD")
	system := systemAccount(t, ctx, pool, "settlement.EUR")

	entryID := uuid.Must(uuid.NewV7())
	err := postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertPostings(ctx, tx, entryID, customer, system, 10000, -10000, "EUR", "EUR")
	})
	requireCheckViolation(t, err)
}

func TestSchema_JournalAppendOnly(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "USD")
	system := systemAccount(t, ctx, pool, "settlement.USD")
	entryID := uuid.Must(uuid.NewV7())
	require.NoError(t, postgres.NewTxManager(pool).WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertPostings(ctx, tx, entryID, customer, system, 10000, -10000, "USD", "USD")
	}))

	_, err := pool.Exec(ctx, `UPDATE postings SET amount = amount WHERE entry_id = $1`, entryID)
	requireAppendOnly(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM postings WHERE entry_id = $1`, entryID)
	requireAppendOnly(t, err)
	_, err = pool.Exec(ctx, `UPDATE journal_entries SET description = 'mutated' WHERE id = $1`, entryID)
	requireAppendOnly(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM journal_entries WHERE id = $1`, entryID)
	requireAppendOnly(t, err)
}

func TestSchema_CustomerAvailableNonNegative(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	customer := insertCustomer(t, ctx, pool, "RUB")

	_, err := pool.Exec(ctx, `UPDATE accounts SET balance = -1 WHERE id = $1`, customer)
	requireCheckViolation(t, err)

	_, err = pool.Exec(ctx, `UPDATE accounts SET held = 1 WHERE id = $1`, customer)
	requireCheckViolation(t, err)
}

func TestSchema_SystemAccountAllowsOverdraft(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())
	system := systemAccount(t, ctx, pool, "settlement.USD")

	_, err := pool.Exec(ctx, `UPDATE accounts SET balance = -500 WHERE id = $1`, system)
	require.NoError(t, err)

	var balance int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, system).Scan(&balance))
	require.Equal(t, int64(-500), balance)
}

func TestSchema_SystemAccountsSeeded(t *testing.T) {
	ctx := testContext(t)
	pool := testenv.MigratedPool(t, migrations.Accounts())

	var n int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM accounts
		WHERE kind = 'system' AND allow_overdraft
		  AND code IN (
		    'settlement.RUB', 'settlement.USD', 'settlement.EUR', 'settlement.CNY', 'settlement.JPY',
		    'fx.RUB', 'fx.USD', 'fx.EUR', 'fx.CNY', 'fx.JPY',
		    'fee.RUB', 'fee.USD', 'fee.EUR', 'fee.CNY', 'fee.JPY'
		  )`).Scan(&n))
	require.Equal(t, 15, n)
}

func insertCustomer(t *testing.T, ctx context.Context, pool *pgxpool.Pool, currency string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	owner := uuid.Must(uuid.NewV7())
	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (id, kind, owner_id, currency)
		VALUES ($1, 'customer', $2, $3)`, id, owner, currency)
	require.NoError(t, err)
	return id
}

func systemAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, code string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := pool.QueryRow(ctx, `SELECT id FROM accounts WHERE code = $1`, code).Scan(&id)
	require.NoError(t, err)
	return id
}

func insertJournal(ctx context.Context, tx pgx.Tx, entryID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, kind, reference_type, reference_id)
		VALUES ($1, 'deposit', 'test', $2)`, entryID, entryID.String())
	return err
}

func insertPostings(ctx context.Context, tx pgx.Tx, entryID, debit, credit uuid.UUID, debitAmt, creditAmt int64, debitCur, creditCur string) error {
	if err := insertJournal(ctx, tx, entryID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
		VALUES ($1, $2, $3, $4, $3)`, entryID, debit, debitAmt, debitCur); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO postings (entry_id, account_id, amount, currency, balance_after)
		VALUES ($1, $2, $3, $4, $3)`, entryID, credit, creditAmt, creditCur)
	return err
}

func requireCheckViolation(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, postgres.CodeCheckViolation, postgres.ErrCode(err))
}

func requireAppendOnly(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, "42501", postgres.ErrCode(err))
}
