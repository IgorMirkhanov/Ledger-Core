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
	now    = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
)

func TestJournalEntryValidate(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	tests := []struct {
		name     string
		postings []Posting
		wantErr  bool
	}{
		{"balanced", []Posting{{a, -100, rub, 0}, {b, 100, rub, 0}}, false},
		{"single posting", []Posting{{a, 100, rub, 0}}, true},
		{"unbalanced", []Posting{{a, -100, rub, 0}, {b, 90, rub, 0}}, true},
		{"zero amount", []Posting{{a, 0, rub, 0}, {b, 0, rub, 0}}, true},
		{"balanced per currency is required", []Posting{{a, -100, rub, 0}, {b, 100, usd, 0}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &JournalEntry{Postings: tt.postings}
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrUnbalancedEntry) {
				t.Fatalf("err = %v, want ErrUnbalancedEntry", err)
			}
		})
	}
}

func TestTransferPostingsFX(t *testing.T) {
	src, dst, fxU, fxR := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ps := TransferPostings(src, dst, money.New(10000, usd), money.New(900000, rub), fxU, fxR)
	if len(ps) != 4 {
		t.Fatalf("got %d postings, want 4", len(ps))
	}
	e := &JournalEntry{Postings: ps}
	if err := e.Validate(); err != nil {
		t.Fatalf("fx entry must be balanced: %v", err)
	}
	if got := TransferPostings(src, dst, money.New(5, rub), money.New(5, rub), fxU, fxR); len(got) != 2 {
		t.Fatalf("same currency must produce 2 postings, got %d", len(got))
	}
}

func TestAccountApplyAndReserve(t *testing.T) {
	acc := NewCustomerAccount(uuid.New(), uuid.New(), rub, now)

	if _, err := acc.Apply(-1, now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("debit empty account err = %v", err)
	}
	if _, err := acc.Apply(1000, now); err != nil {
		t.Fatal(err)
	}
	if err := acc.Reserve(600, now); err != nil {
		t.Fatal(err)
	}
	if acc.Available() != 400 {
		t.Fatalf("available = %d, want 400", acc.Available())
	}
	if err := acc.Reserve(500, now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("over-reserve err = %v", err)
	}
	if _, err := acc.Apply(-500, now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("debit over available err = %v", err)
	}
	acc.Status = StatusFrozen
	if err := acc.Reserve(1, now); !errors.Is(err, ErrAccountNotActive) {
		t.Fatalf("reserve on frozen err = %v", err)
	}
}

func TestSystemAccountOverdraft(t *testing.T) {
	acc := &Account{ID: uuid.New(), Kind: KindSystem, Currency: rub, Status: StatusActive, AllowOverdraft: true}
	if _, err := acc.Apply(-1_000_000, now); err != nil {
		t.Fatalf("system account must allow overdraft: %v", err)
	}
}

func TestHoldLifecycle(t *testing.T) {
	h := &Hold{ID: uuid.New(), Amount: 100, Status: HoldActive, ExpiresAt: now.Add(time.Minute)}

	if err := h.Capture(uuid.New(), now.Add(2*time.Minute)); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("capture expired err = %v", err)
	}
	if err := h.Capture(uuid.New(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Release(now); !errors.Is(err, ErrHoldNotActive) {
		t.Fatalf("release captured err = %v", err)
	}

	h2 := &Hold{ID: uuid.New(), Amount: 100, Status: HoldActive, ExpiresAt: now.Add(time.Minute)}
	if changed, err := h2.Release(now); err != nil || !changed {
		t.Fatalf("release = %v, %v", changed, err)
	}
	if changed, err := h2.Release(now); err != nil || changed {
		t.Fatalf("second release must be a no-op: %v, %v", changed, err)
	}
}
