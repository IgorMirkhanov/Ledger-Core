// Package reconciler verifies ledger invariants against the accounts database.
package reconciler

import (
	"context"
	"fmt"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/postgres"
)

// Discrepancy is one invariant violation found by a check.
type Discrepancy struct {
	CheckName string
	SubjectID string
	Expected  string
	Actual    string
}

// Check is one aggregating invariant query.
type Check interface {
	Name() string
	Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error)
}

// DefaultChecks returns checks 1–5 from ADR-0007.
func DefaultChecks() []Check {
	return []Check{
		checkG1{},
		checkG2{},
		checkG9{},
		checkG3{},
		checkExpiredHolds{},
	}
}

type checkG1 struct{}

func (checkG1) Name() string { return "g1_journal_balanced" }

func (checkG1) Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error) {
	rows, err := q.Query(ctx, `
		WITH per_currency AS (
			SELECT entry_id, currency, SUM(amount) AS sum, COUNT(*) AS cnt
			FROM postings
			GROUP BY entry_id, currency
		)
		SELECT entry_id::text, '0', currency || ':sum=' || sum::text || ',cnt=' || cnt::text
		FROM per_currency
		WHERE sum <> 0
		UNION ALL
		SELECT e.id::text, '>=2 postings', 'postings=' || (
			SELECT COUNT(*)::text FROM postings p WHERE p.entry_id = e.id
		)
		FROM journal_entries e
		WHERE (SELECT COUNT(*) FROM postings p WHERE p.entry_id = e.id) < 2`)
	if err != nil {
		return nil, fmt.Errorf("g1: %w", err)
	}
	defer rows.Close()
	return scanDisc(rows, "g1_journal_balanced")
}

type checkG2 struct{}

func (checkG2) Name() string { return "g2_currency_zero_sum" }

func (checkG2) Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT currency, '0', SUM(balance)::text
		FROM accounts
		GROUP BY currency
		HAVING SUM(balance) <> 0`)
	if err != nil {
		return nil, fmt.Errorf("g2: %w", err)
	}
	defer rows.Close()
	return scanDisc(rows, "g2_currency_zero_sum")
}

type checkG9 struct{}

func (checkG9) Name() string { return "g9_balance_equals_postings" }

func (checkG9) Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT a.id::text,
		       COALESCE(SUM(p.amount), 0)::text,
		       a.balance::text
		FROM accounts a
		LEFT JOIN postings p ON p.account_id = a.id
		GROUP BY a.id, a.balance
		HAVING a.balance <> COALESCE(SUM(p.amount), 0)
		UNION
		SELECT a.id::text,
		       COALESCE(lp.balance_after::text, '0'),
		       a.balance::text
		FROM accounts a
		LEFT JOIN LATERAL (
			SELECT balance_after FROM postings
			WHERE account_id = a.id
			ORDER BY id DESC
			LIMIT 1
		) lp ON true
		WHERE (lp.balance_after IS NULL AND a.balance <> 0)
		   OR (lp.balance_after IS NOT NULL AND a.balance <> lp.balance_after)`)
	if err != nil {
		return nil, fmt.Errorf("g9: %w", err)
	}
	defer rows.Close()
	return scanDisc(rows, "g9_balance_equals_postings")
}

type checkG3 struct{}

func (checkG3) Name() string { return "g3_held_equals_active_holds" }

func (checkG3) Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT a.id::text,
		       COALESCE(SUM(h.amount), 0)::text,
		       a.held::text
		FROM accounts a
		LEFT JOIN holds h ON h.account_id = a.id AND h.status = 'active'
		GROUP BY a.id, a.held
		HAVING a.held <> COALESCE(SUM(h.amount), 0)`)
	if err != nil {
		return nil, fmt.Errorf("g3: %w", err)
	}
	defer rows.Close()
	return scanDisc(rows, "g3_held_equals_active_holds")
}

type checkExpiredHolds struct{}

func (checkExpiredHolds) Name() string { return "expired_active_holds" }

func (checkExpiredHolds) Run(ctx context.Context, q postgres.Querier) ([]Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT id::text,
		       'expires_at >= now()-5m',
		       expires_at::text
		FROM holds
		WHERE status = 'active' AND expires_at < now() - interval '5 minutes'`)
	if err != nil {
		return nil, fmt.Errorf("expired_holds: %w", err)
	}
	defer rows.Close()
	return scanDisc(rows, "expired_active_holds")
}

type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanDisc(rows rowScanner, name string) ([]Discrepancy, error) {
	var out []Discrepancy
	for rows.Next() {
		var d Discrepancy
		d.CheckName = name
		if err := rows.Scan(&d.SubjectID, &d.Expected, &d.Actual); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
