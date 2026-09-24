package gateway

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var amountRe = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)

// ParseAmountString parses a JSON money string into minor units.
// Accepts only unquoted-looking digit strings that fit in int64 and are > 0.
func ParseAmountString(s string) (int64, error) {
	if !amountRe.MatchString(s) {
		return 0, fmt.Errorf("invalid amount %q", s)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", s)
	}
	return n, nil
}

func FormatAmount(n int64) string {
	return strconv.FormatInt(n, 10)
}

// FormatFXRate strips trailing zeros from a decimal rate string ("90.000" → "90",
// "0.01100" → "0.011"). Empty input yields empty string (caller should omit the field).
func FormatFXRate(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" || s == "-" {
		return "0"
	}
	return s
}
