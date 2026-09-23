// Package money is the only place where arithmetic on monetary amounts is allowed.
//
// Amounts are int64 minor units (kopecks, cents). Floats are never used.
package money

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
)

var (
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: amount overflow")
	ErrNonPositive      = errors.New("money: amount must be positive")
	ErrInvalidRate      = errors.New("money: invalid fx rate")
)

// Currency is an ISO 4217 code with its minor-unit exponent.
type Currency struct {
	Code       string
	MinorUnits int
}

var currencyCodeRe = regexp.MustCompile(`^[A-Z]{3}$`)

// Known currencies. Must stay in sync with the `currencies` table seed.
var known = map[string]Currency{
	"RUB": {Code: "RUB", MinorUnits: 2},
	"USD": {Code: "USD", MinorUnits: 2},
	"EUR": {Code: "EUR", MinorUnits: 2},
	"CNY": {Code: "CNY", MinorUnits: 2},
	"JPY": {Code: "JPY", MinorUnits: 0},
}

// ParseCurrency returns a known currency by its ISO code.
func ParseCurrency(code string) (Currency, error) {
	if !currencyCodeRe.MatchString(code) {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	c, ok := known[code]
	if !ok {
		return Currency{}, fmt.Errorf("%w: unsupported %q", ErrInvalidCurrency, code)
	}
	return c, nil
}

func (c Currency) String() string { return c.Code }

// Money is an amount in minor units of a currency. The zero value is invalid.
type Money struct {
	amount   int64
	currency Currency
}

// New creates Money from minor units.
func New(amount int64, currency Currency) Money {
	return Money{amount: amount, currency: currency}
}

// NewPositive creates Money and requires amount > 0 (typical for request validation).
func NewPositive(amount int64, currencyCode string) (Money, error) {
	c, err := ParseCurrency(currencyCode)
	if err != nil {
		return Money{}, err
	}
	if amount <= 0 {
		return Money{}, ErrNonPositive
	}
	return New(amount, c), nil
}

func (m Money) Amount() int64      { return m.amount }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsZero() bool       { return m.amount == 0 }
func (m Money) IsNegative() bool   { return m.amount < 0 }
func (m Money) IsPositive() bool   { return m.amount > 0 }

func (m Money) String() string {
	return fmt.Sprintf("%d %s(minor)", m.amount, m.currency.Code)
}

// Add returns m + o. Currencies must match; overflow is an error.
func (m Money) Add(o Money) (Money, error) {
	if m.currency.Code != o.currency.Code {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency.Code, o.currency.Code)
	}
	sum, err := AddInt64(m.amount, o.amount)
	if err != nil {
		return Money{}, err
	}
	return New(sum, m.currency), nil
}

// Sub returns m - o. Currencies must match; overflow is an error.
func (m Money) Sub(o Money) (Money, error) {
	if o.amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return m.Add(New(-o.amount, o.currency))
}

// Neg returns -m.
func (m Money) Neg() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return New(-m.amount, m.currency), nil
}

// AddInt64 adds two int64 values and reports overflow.
func AddInt64(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// ParseRate parses a decimal FX rate string ("90.5") into an exact rational.
func ParseRate(s string) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok || r.Sign() <= 0 {
		return nil, fmt.Errorf("%w: %q", ErrInvalidRate, s)
	}
	return r, nil
}

// Convert converts m into currency `to` using `rate` (units of `to` per 1 major unit of m's currency).
// Minor-unit exponents are taken into account; the result is rounded half-to-even (banker's rounding).
//
//	100.00 USD (10000 minor) * 90 → 9000.00 RUB (900000 minor)
func Convert(m Money, to Currency, rate *big.Rat) (Money, error) {
	if rate == nil || rate.Sign() <= 0 {
		return Money{}, ErrInvalidRate
	}
	// result_minor = amount_minor * rate * 10^(to.exp - from.exp)
	v := new(big.Rat).SetInt64(m.amount)
	v.Mul(v, rate)
	exp := to.MinorUnits - m.currency.MinorUnits
	scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exp))), nil))
	if exp >= 0 {
		v.Mul(v, scale)
	} else {
		v.Quo(v, scale)
	}
	rounded := roundHalfEven(v)
	if !rounded.IsInt64() {
		return Money{}, ErrOverflow
	}
	return New(rounded.Int64(), to), nil
}

func roundHalfEven(r *big.Rat) *big.Int {
	num, den := r.Num(), r.Denom()
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	if rem.Sign() == 0 {
		return q
	}
	// Compare 2*|rem| with den.
	twice := new(big.Int).Mul(new(big.Int).Abs(rem), big.NewInt(2))
	cmp := twice.Cmp(den)
	awayFromZero := cmp > 0 || (cmp == 0 && q.Bit(0) == 1)
	if awayFromZero {
		if r.Sign() > 0 {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
