// Package domain contains the transfer saga state machine. No I/O here.
package domain

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

type Status string

const (
	StatusCreated      Status = "created"
	StatusFundsHeld    Status = "funds_held"
	StatusCompleted    Status = "completed"
	StatusCompensating Status = "compensating"
	StatusFailed       Status = "failed"
)

func (s Status) IsTerminal() bool { return s == StatusCompleted || s == StatusFailed }

// Step is the next saga action required to move the transfer forward.
type Step string

const (
	StepNone    Step = ""
	StepHold    Step = "hold"
	StepCapture Step = "capture"
	StepRelease Step = "release"
)

var (
	ErrInvalidTransition = errors.New("INVALID_TRANSITION")
	ErrSameAccount       = errors.New("SAME_ACCOUNT")
	ErrValidation        = errors.New("VALIDATION_FAILED")
	ErrNotFound          = errors.New("TRANSFER_NOT_FOUND")
	ErrFXRateNotFound    = errors.New("FX_RATE_NOT_FOUND")
)

// Failure codes stored in transfers.failure_code (mirror accounts' ErrorInfo reasons).
const (
	FailureInsufficientFunds = "INSUFFICIENT_FUNDS"
	FailureAccountNotActive  = "ACCOUNT_NOT_ACTIVE"
	FailureHoldExpired       = "HOLD_EXPIRED"
	FailureCurrencyMismatch  = "CURRENCY_MISMATCH"
	FailureAccountNotFound   = "ACCOUNT_NOT_FOUND"
	FailureNotAccountOwner   = "NOT_ACCOUNT_OWNER"
)

// Transfer is the saga aggregate.
type Transfer struct {
	ID              uuid.UUID
	OwnerID         uuid.UUID
	SourceAccountID uuid.UUID
	DestAccountID   uuid.UUID
	Amount          money.Money
	DestAmount      money.Money
	FXRate          *big.Rat // nil when currencies match
	Status          Status
	HoldID          uuid.UUID
	JournalEntryID  uuid.UUID
	FailureCode     string
	FailureReason   string
	Attempts        int
	NextAttemptAt   *time.Time
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     *time.Time
}

// NewTransfer validates input and creates a transfer in status created.
// rate must be non-nil iff currencies differ.
func NewTransfer(id, owner, src, dst uuid.UUID, amount money.Money, destCur money.Currency, rate *big.Rat, now time.Time) (*Transfer, error) {
	if src == dst {
		return nil, ErrSameAccount
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: amount must be positive", ErrValidation)
	}
	destAmount := amount
	if amount.Currency().Code != destCur.Code {
		if rate == nil {
			return nil, ErrFXRateNotFound
		}
		var err error
		destAmount, err = money.Convert(amount, destCur, rate)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrValidation, err)
		}
		if !destAmount.IsPositive() {
			return nil, fmt.Errorf("%w: converted amount rounds to zero", ErrValidation)
		}
	} else {
		rate = nil
	}
	return &Transfer{
		ID:              id,
		OwnerID:         owner,
		SourceAccountID: src,
		DestAccountID:   dst,
		Amount:          amount,
		DestAmount:      destAmount,
		FXRate:          rate,
		Status:          StatusCreated,
		CreatedAt:       now,
		UpdatedAt:       now,
		NextAttemptAt:   &now,
	}, nil
}

// NextStep returns what the orchestrator must do next.
func (t *Transfer) NextStep() Step {
	switch t.Status {
	case StatusCreated:
		return StepHold
	case StatusFundsHeld:
		return StepCapture
	case StatusCompensating:
		return StepRelease
	default:
		return StepNone
	}
}

// IdempotencyKey returns the deterministic key used for the downstream call of a step.
// Retrying a step after a timeout reuses the same key, so accounts executes it at most once.
func (t *Transfer) IdempotencyKey(step Step) string {
	return "transfer:" + t.ID.String() + ":" + string(step)
}

// --- transitions ---------------------------------------------------------

func (t *Transfer) OnHoldCreated(holdID uuid.UUID, now time.Time) error {
	if t.Status != StatusCreated {
		return t.invalid(StatusFundsHeld)
	}
	t.Status = StatusFundsHeld
	t.HoldID = holdID
	t.touch(now)
	t.NextAttemptAt = &now
	return nil
}

// OnHoldRejected: business refusal before any money was reserved → failed, nothing to compensate.
func (t *Transfer) OnHoldRejected(code, reason string, now time.Time) error {
	if t.Status != StatusCreated {
		return t.invalid(StatusFailed)
	}
	t.fail(code, reason, now)
	return nil
}

func (t *Transfer) OnCaptured(entryID uuid.UUID, now time.Time) error {
	if t.Status != StatusFundsHeld {
		return t.invalid(StatusCompleted)
	}
	t.Status = StatusCompleted
	t.JournalEntryID = entryID
	t.CompletedAt = &now
	t.NextAttemptAt = nil
	t.touch(now)
	return nil
}

// OnCaptureRejected: money is reserved but cannot be moved → compensate by releasing the hold.
func (t *Transfer) OnCaptureRejected(code, reason string, now time.Time) error {
	if t.Status != StatusFundsHeld {
		return t.invalid(StatusCompensating)
	}
	t.Status = StatusCompensating
	t.FailureCode = code
	t.FailureReason = reason
	t.NextAttemptAt = &now
	t.touch(now)
	return nil
}

func (t *Transfer) OnReleased(now time.Time) error {
	if t.Status != StatusCompensating {
		return t.invalid(StatusFailed)
	}
	t.fail(t.FailureCode, t.FailureReason, now)
	return nil
}

// OnTransientError schedules a retry with exponential backoff; status does not change.
func (t *Transfer) OnTransientError(now time.Time, backoff func(attempt int) time.Duration) {
	t.Attempts++
	next := now.Add(backoff(t.Attempts))
	t.NextAttemptAt = &next
	t.touch(now)
}

func (t *Transfer) fail(code, reason string, now time.Time) {
	t.Status = StatusFailed
	t.FailureCode = code
	t.FailureReason = reason
	t.CompletedAt = &now
	t.NextAttemptAt = nil
	t.touch(now)
}

func (t *Transfer) touch(now time.Time) {
	t.UpdatedAt = now
	t.Version++
}

func (t *Transfer) invalid(to Status) error {
	return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, t.Status, to)
}
