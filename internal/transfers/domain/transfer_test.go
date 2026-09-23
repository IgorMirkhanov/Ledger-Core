package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/money"
)

var (
	rub, _ = money.ParseCurrency("RUB")
	usd, _ = money.ParseCurrency("USD")
	now    = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

func newT(t *testing.T) *Transfer {
	t.Helper()
	tr, err := NewTransfer(uuid.New(), uuid.New(), uuid.New(), uuid.New(), money.New(1000, rub), rub, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestHappyPath(t *testing.T) {
	tr := newT(t)
	if tr.NextStep() != StepHold {
		t.Fatalf("step = %q", tr.NextStep())
	}
	must(t, tr.OnHoldCreated(uuid.New(), now))
	if tr.NextStep() != StepCapture {
		t.Fatalf("step = %q", tr.NextStep())
	}
	must(t, tr.OnCaptured(uuid.New(), now))
	if tr.Status != StatusCompleted || tr.NextStep() != StepNone || tr.NextAttemptAt != nil {
		t.Fatalf("unexpected final state: %+v", tr)
	}
}

func TestHoldRejected(t *testing.T) {
	tr := newT(t)
	must(t, tr.OnHoldRejected(FailureInsufficientFunds, "no money", now))
	if tr.Status != StatusFailed || tr.FailureCode != FailureInsufficientFunds {
		t.Fatalf("state = %+v", tr)
	}
}

func TestCompensation(t *testing.T) {
	tr := newT(t)
	must(t, tr.OnHoldCreated(uuid.New(), now))
	must(t, tr.OnCaptureRejected(FailureAccountNotActive, "dest frozen", now))
	if tr.NextStep() != StepRelease {
		t.Fatalf("step = %q", tr.NextStep())
	}
	must(t, tr.OnReleased(now))
	if tr.Status != StatusFailed || tr.FailureCode != FailureAccountNotActive {
		t.Fatalf("state = %+v", tr)
	}
}

func TestInvalidTransitions(t *testing.T) {
	tr := newT(t)
	if err := tr.OnCaptured(uuid.New(), now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("capture from created err = %v", err)
	}
	must(t, tr.OnHoldCreated(uuid.New(), now))
	must(t, tr.OnCaptured(uuid.New(), now))
	if err := tr.OnCaptureRejected("X", "", now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("reject after completed err = %v", err)
	}
}

func TestTransientErrorBackoff(t *testing.T) {
	tr := newT(t)
	tr.OnTransientError(now, func(a int) time.Duration { return time.Duration(a) * time.Second })
	tr.OnTransientError(now, func(a int) time.Duration { return time.Duration(a) * time.Second })
	if tr.Status != StatusCreated || tr.Attempts != 2 || !tr.NextAttemptAt.Equal(now.Add(2*time.Second)) {
		t.Fatalf("state = %+v", tr)
	}
}

func TestNewTransferValidation(t *testing.T) {
	a := uuid.New()
	if _, err := NewTransfer(uuid.New(), uuid.New(), a, a, money.New(1, rub), rub, nil, now); !errors.Is(err, ErrSameAccount) {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewTransfer(uuid.New(), uuid.New(), uuid.New(), uuid.New(), money.New(100, usd), rub, nil, now); !errors.Is(err, ErrFXRateNotFound) {
		t.Fatalf("err = %v", err)
	}
	rate, _ := money.ParseRate("90")
	tr, err := NewTransfer(uuid.New(), uuid.New(), uuid.New(), uuid.New(), money.New(10000, usd), rub, rate, now)
	if err != nil || tr.DestAmount.Amount() != 900000 {
		t.Fatalf("fx transfer = %+v, %v", tr, err)
	}
}

func TestIdempotencyKeyIsDeterministic(t *testing.T) {
	tr := newT(t)
	clone := *tr
	if tr.IdempotencyKey(StepHold) != clone.IdempotencyKey(StepHold) || tr.IdempotencyKey(StepHold) == tr.IdempotencyKey(StepCapture) {
		t.Fatal("keys must be deterministic per step and distinct across steps")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
