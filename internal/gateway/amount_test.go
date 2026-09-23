package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAmountString(t *testing.T) {
	ok, err := ParseAmountString("10050")
	require.NoError(t, err)
	require.Equal(t, int64(10050), ok)
	require.Equal(t, "10050", FormatAmount(ok))

	cases := []string{"0", "-5", "+5", "1.5", "1e3", " 5", "05", "", "9223372036854775808"}
	for _, s := range cases {
		_, err := ParseAmountString(s)
		require.Error(t, err, s)
	}
}
