package domain

import (
	"errors"
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
	BalanceAfter int64 // set by JournalEntry.Apply; 0 is a legitimate value
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

	applied bool
}

// ErrEntryNotApplied is returned by persistence when an entry is saved before Apply.
var ErrEntryNotApplied = errors.New("ENTRY_NOT_APPLIED") // programming error

// Apply validates the entry and applies every posting to its (already locked) account in order,
// setting BalanceAfter. It is all-or-nothing: on error no account is modified.
// Holds must be unreserved BEFORE Apply (see CaptureHold in docs/database.md).
func (e *JournalEntry) Apply(accounts map[uuid.UUID]*Account, now time.Time) error {
	if e.applied {
		return fmt.Errorf("%w: entry already applied", ErrUnbalancedEntry)
	}
	if err := e.Validate(); err != nil {
		return err
	}
	// Dry run on copies so a failure in the middle leaves accounts untouched.
	scratch := make(map[uuid.UUID]Account, len(accounts))
	for i, p := range e.Postings {
		acc, ok := accounts[p.AccountID]
		if !ok {
			return fmt.Errorf("%w: account %s is not locked for posting %d", ErrAccountNotFound, p.AccountID, i)
		}
		if acc.Currency.Code != p.Currency.Code {
			return ErrCurrencyMismatch
		}
		if p.Amount > 0 {
			if err := acc.CanCredit(); err != nil {
				return err
			}
		} else if acc.Status != StatusActive {
			return ErrAccountNotActive
		}
		if _, seen := scratch[p.AccountID]; !seen {
			scratch[p.AccountID] = *acc
		}
		c := scratch[p.AccountID]
		if _, err := c.Apply(p.Amount, now); err != nil {
			return err
		}
		scratch[p.AccountID] = c
	}
	for i := range e.Postings {
		p := &e.Postings[i]
		bal, err := accounts[p.AccountID].Apply(p.Amount, now)
		if err != nil {
			return err // unreachable: dry run succeeded
		}
		p.BalanceAfter = bal
	}
	e.applied = true
	return nil
}

// IsApplied reports whether Apply succeeded. Repositories must refuse to persist unapplied entries.
func (e *JournalEntry) IsApplied() bool { return e.applied }

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
