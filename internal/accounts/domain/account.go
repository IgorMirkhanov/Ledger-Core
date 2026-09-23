package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

type AccountKind string

const (
	KindCustomer AccountKind = "customer"
	KindSystem   AccountKind = "system"
)

type AccountStatus string

const (
	StatusActive AccountStatus = "active"
	StatusFrozen AccountStatus = "frozen"
	StatusClosed AccountStatus = "closed"
)

// System account code prefixes. Full code is "<prefix>.<CUR>", e.g. "settlement.RUB".
const (
	SystemSettlement = "settlement"
	SystemFX         = "fx"
	SystemFee        = "fee"
)

// SystemCode returns the code of a system account for the given prefix and currency.
func SystemCode(prefix string, c money.Currency) string { return prefix + "." + c.Code }

// Account is a ledger account. Balance and Held are in minor units of Currency.
type Account struct {
	ID             uuid.UUID
	Kind           AccountKind
	OwnerID        uuid.UUID // uuid.Nil for system accounts
	Code           string    // empty for customer accounts
	Currency       money.Currency
	Status         AccountStatus
	Balance        int64
	Held           int64
	AllowOverdraft bool
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewCustomerAccount creates a new empty customer account.
func NewCustomerAccount(id, ownerID uuid.UUID, currency money.Currency, now time.Time) *Account {
	return &Account{
		ID:        id,
		Kind:      KindCustomer,
		OwnerID:   ownerID,
		Currency:  currency,
		Status:    StatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Available returns balance minus active holds.
func (a *Account) Available() int64 { return a.Balance - a.Held }

// CheckOwner verifies that the account belongs to ownerID.
func (a *Account) CheckOwner(ownerID uuid.UUID) error {
	if a.Kind != KindCustomer || a.OwnerID != ownerID {
		return ErrNotAccountOwner
	}
	return nil
}

// CanDebit reports whether amount can be taken from the account right now (ignoring holds of the caller).
func (a *Account) CanDebit(amount int64) error {
	if a.Status != StatusActive {
		return ErrAccountNotActive
	}
	if !a.AllowOverdraft && a.Available() < amount {
		return ErrInsufficientFunds
	}
	return nil
}

// CanCredit reports whether money can be put on the account.
func (a *Account) CanCredit() error {
	if a.Status == StatusClosed {
		return ErrAccountNotActive
	}
	return nil
}

// Apply changes the balance by a signed amount and returns the new balance.
// It enforces the overdraft rule; holds are NOT consumed here (see Hold.Capture).
func (a *Account) Apply(amount int64, now time.Time) (int64, error) {
	newBalance, err := money.AddInt64(a.Balance, amount)
	if err != nil {
		return 0, err
	}
	if !a.AllowOverdraft && newBalance-a.Held < 0 {
		return 0, ErrInsufficientFunds
	}
	a.Balance = newBalance
	a.Version++
	a.UpdatedAt = now
	return newBalance, nil
}

// Reserve increases Held (used when creating a hold).
func (a *Account) Reserve(amount int64, now time.Time) error {
	if amount <= 0 {
		return fmt.Errorf("%w: reserve amount must be positive", ErrValidation)
	}
	if err := a.CanDebit(amount); err != nil {
		return err
	}
	a.Held += amount
	a.Version++
	a.UpdatedAt = now
	return nil
}

// Unreserve decreases Held (used on release/expire/capture of a hold).
func (a *Account) Unreserve(amount int64, now time.Time) error {
	if amount <= 0 || amount > a.Held {
		return fmt.Errorf("%w: cannot unreserve %d, held %d", ErrValidation, amount, a.Held)
	}
	a.Held -= amount
	a.Version++
	a.UpdatedAt = now
	return nil
}
