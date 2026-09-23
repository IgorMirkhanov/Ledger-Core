package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

type EntryKind string

const (
	EntryDeposit    EntryKind = "deposit"
	EntryWithdrawal EntryKind = "withdrawal"
	EntryTransfer   EntryKind = "transfer"
	EntryReversal   EntryKind = "reversal"
)

// Posting changes the balance of one account by a signed amount.
type Posting struct {
	AccountID    uuid.UUID
	Amount       int64 // + increases account balance, − decreases
	Currency     money.Currency
	BalanceAfter int64 // filled by the service after applying to the account
}

// JournalEntry is one business operation consisting of balanced postings.
type JournalEntry struct {
	ID            uuid.UUID
	Kind          EntryKind
	ReferenceType string
	ReferenceID   string
	Description   string
	Postings      []Posting
	CreatedAt     time.Time
}

// Validate enforces the double-entry invariant (G1):
// at least two postings, no zero amounts, and per-currency sum equal to zero.
// The same invariant is enforced by a deferred trigger in Postgres; this is the first line of defense.
func (e *JournalEntry) Validate() error {
	if len(e.Postings) < 2 {
		return fmt.Errorf("%w: entry needs at least 2 postings, got %d", ErrUnbalancedEntry, len(e.Postings))
	}
	sums := make(map[string]int64, 2)
	for i, p := range e.Postings {
		if p.Amount == 0 {
			return fmt.Errorf("%w: posting %d has zero amount", ErrUnbalancedEntry, i)
		}
		s, err := money.AddInt64(sums[p.Currency.Code], p.Amount)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUnbalancedEntry, err)
		}
		sums[p.Currency.Code] = s
	}
	for cur, s := range sums {
		if s != 0 {
			return fmt.Errorf("%w: %s sum is %d", ErrUnbalancedEntry, cur, s)
		}
	}
	return nil
}

// AccountIDs returns the distinct account IDs touched by the entry (unordered).
func (e *JournalEntry) AccountIDs() []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(e.Postings))
	ids := make([]uuid.UUID, 0, len(e.Postings))
	for _, p := range e.Postings {
		if _, ok := seen[p.AccountID]; !ok {
			seen[p.AccountID] = struct{}{}
			ids = append(ids, p.AccountID)
		}
	}
	return ids
}

// TransferPostings builds postings for moving money between accounts.
// Same currency: 2 postings. Different currency: 4 postings via fx.<CUR> accounts.
//
//	src   −srcAmount (srcCur)
//	fxSrc +srcAmount (srcCur)   ┐ only when currencies differ
//	fxDst −dstAmount (dstCur)   ┘
//	dst   +dstAmount (dstCur)
func TransferPostings(src, dst uuid.UUID, srcAmount, dstAmount money.Money, fxSrc, fxDst uuid.UUID) []Posting {
	if srcAmount.Currency().Code == dstAmount.Currency().Code {
		return []Posting{
			{AccountID: src, Amount: -srcAmount.Amount(), Currency: srcAmount.Currency()},
			{AccountID: dst, Amount: srcAmount.Amount(), Currency: srcAmount.Currency()},
		}
	}
	return []Posting{
		{AccountID: src, Amount: -srcAmount.Amount(), Currency: srcAmount.Currency()},
		{AccountID: fxSrc, Amount: srcAmount.Amount(), Currency: srcAmount.Currency()},
		{AccountID: fxDst, Amount: -dstAmount.Amount(), Currency: dstAmount.Currency()},
		{AccountID: dst, Amount: dstAmount.Amount(), Currency: dstAmount.Currency()},
	}
}
