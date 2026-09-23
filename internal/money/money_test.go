package money

import (
	"errors"
	"math"
	"testing"
)

func mustCur(t *testing.T, code string) Currency {
	t.Helper()
	c, err := ParseCurrency(code)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseCurrency(t *testing.T) {
	for _, code := range []string{"", "rub", "RU", "XXX"} {
		if _, err := ParseCurrency(code); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) err = %v, want ErrInvalidCurrency", code, err)
		}
	}
	if c := mustCur(t, "JPY"); c.MinorUnits != 0 {
		t.Errorf("JPY minor units = %d", c.MinorUnits)
	}
}

func TestAddSub(t *testing.T) {
	rub, usd := mustCur(t, "RUB"), mustCur(t, "USD")

	sum, err := New(100, rub).Add(New(50, rub))
	if err != nil || sum.Amount() != 150 {
		t.Fatalf("Add = %v, %v", sum, err)
	}
	if _, err := New(1, rub).Add(New(1, usd)); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("mismatch err = %v", err)
	}
	if _, err := New(math.MaxInt64, rub).Add(New(1, rub)); !errors.Is(err, ErrOverflow) {
		t.Errorf("overflow err = %v", err)
	}
	if _, err := New(math.MinInt64, rub).Sub(New(1, rub)); !errors.Is(err, ErrOverflow) {
		t.Errorf("underflow err = %v", err)
	}
	if _, err := New(0, rub).Sub(New(math.MinInt64, rub)); !errors.Is(err, ErrOverflow) {
		t.Errorf("sub MinInt64 err = %v", err)
	}
}

func TestNewPositive(t *testing.T) {
	if _, err := NewPositive(0, "RUB"); !errors.Is(err, ErrNonPositive) {
		t.Errorf("err = %v", err)
	}
	if _, err := NewPositive(-5, "RUB"); !errors.Is(err, ErrNonPositive) {
		t.Errorf("err = %v", err)
	}
	if m, err := NewPositive(5, "RUB"); err != nil || m.Amount() != 5 {
		t.Errorf("m = %v err = %v", m, err)
	}
}

func TestConvert(t *testing.T) {
	tests := []struct {
		name     string
		amount   int64
		from, to string
		rate     string
		want     int64
	}{
		{"usd to rub", 10000, "USD", "RUB", "90", 900000},
		{"rub to usd", 100000, "RUB", "USD", "0.011", 1100},
		{"to zero-exponent currency", 12345, "USD", "JPY", "150", 18518}, // 123.45*150 = 18517.5 → 18518 (half-even, 18517 odd)
		{"half even down", 5, "USD", "JPY", "1", 0},                      // 0.05 → 0
		{"half even exact .5 even", 50, "USD", "JPY", "1", 0},            // 0.5 → 0
		{"half even exact .5 odd", 150, "USD", "JPY", "1", 2},            // 1.5 → 2
		{"from zero-exponent", 100, "JPY", "USD", "0.0067", 67},          // 100 JPY → 0.67 USD
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rate, err := ParseRate(tt.rate)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Convert(New(tt.amount, mustCur(t, tt.from)), mustCur(t, tt.to), rate)
			if err != nil {
				t.Fatal(err)
			}
			if got.Amount() != tt.want || got.Currency().Code != tt.to {
				t.Errorf("Convert = %d %s, want %d %s", got.Amount(), got.Currency().Code, tt.want, tt.to)
			}
		})
	}
}

func TestParseRateInvalid(t *testing.T) {
	for _, s := range []string{"", "abc", "0", "-1"} {
		if _, err := ParseRate(s); !errors.Is(err, ErrInvalidRate) {
			t.Errorf("ParseRate(%q) err = %v", s, err)
		}
	}
}
