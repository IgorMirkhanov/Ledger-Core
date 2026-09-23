package domain

import (
	"time"

	"github.com/google/uuid"
)

type HoldStatus string

const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldReleased HoldStatus = "released"
	HoldExpired  HoldStatus = "expired"
)

const (
	DefaultHoldTTL = 15 * time.Minute
	MaxHoldTTL     = 7 * 24 * time.Hour
)

// Hold reserves funds on an account until it is captured, released or expired.
type Hold struct {
	ID             uuid.UUID
	AccountID      uuid.UUID
	Amount         int64
	Status         HoldStatus
	ReferenceID    string
	ExpiresAt      time.Time
	JournalEntryID uuid.UUID // set when captured
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (h *Hold) IsTerminal() bool { return h.Status != HoldActive }

// IsExpired reports whether an active hold is past its expiry.
func (h *Hold) IsExpired(now time.Time) bool {
	return h.Status == HoldActive && !now.Before(h.ExpiresAt)
}

// Capture transitions active → captured.
// An expired-but-not-yet-swept hold cannot be captured: the caller must treat it as HOLD_EXPIRED.
func (h *Hold) Capture(entryID uuid.UUID, now time.Time) error {
	if h.Status != HoldActive {
		return ErrHoldNotActive
	}
	if h.IsExpired(now) {
		return ErrHoldExpired
	}
	h.Status = HoldCaptured
	h.JournalEntryID = entryID
	h.UpdatedAt = now
	return nil
}

// Release transitions active → released. Releasing an already released hold is a no-op (idempotent).
func (h *Hold) Release(now time.Time) (changed bool, err error) {
	switch h.Status {
	case HoldReleased, HoldExpired:
		return false, nil
	case HoldCaptured:
		return false, ErrHoldNotActive
	}
	h.Status = HoldReleased
	h.UpdatedAt = now
	return true, nil
}

// Expire transitions active → expired (used by the hold expirer).
func (h *Hold) Expire(now time.Time) error {
	if h.Status != HoldActive {
		return ErrHoldNotActive
	}
	h.Status = HoldExpired
	h.UpdatedAt = now
	return nil
}
