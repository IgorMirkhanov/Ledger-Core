package kafka

import (
	"testing"
	"time"
)

func TestValidateRetryBudget(t *testing.T) {
	if err := validateRetryBudget(5, defaultBackoff); err != nil {
		t.Fatal(err)
	}
	if err := validateRetryBudget(5, func(int) time.Duration { return 6 * time.Second }); err != nil {
		t.Fatal(err)
	}
	if err := validateRetryBudget(5, func(int) time.Duration { return 7 * time.Second }); err == nil {
		t.Fatal("expected retry budget to be rejected")
	}
	if err := validateRetryBudget(1, func(int) time.Duration { return -time.Millisecond }); err == nil {
		t.Fatal("expected negative backoff to be rejected")
	}
}
