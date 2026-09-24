package notifications_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/IgorMirkhanov/ledger-core/internal/notifications"
)

func TestSendBackoff(t *testing.T) {
	require.Equal(t, 5*time.Second, notifications.SendBackoff(1))
	require.Equal(t, 10*time.Second, notifications.SendBackoff(2))
	require.Equal(t, 20*time.Second, notifications.SendBackoff(3))
	require.Equal(t, 40*time.Second, notifications.SendBackoff(4))
	require.Equal(t, 80*time.Second, notifications.SendBackoff(5))
	require.Equal(t, 10*time.Minute, notifications.SendBackoff(20))
}
