//go:build integration

package testenv

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

var (
	rpOnce      sync.Once
	rpErr       error
	rpContainer *redpanda.Container
	rpBrokers   []string
)

// StartRedpanda returns the Kafka seed broker of a shared Redpanda container.
func StartRedpanda(t *testing.T) []string {
	t.Helper()
	rpOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		rpErr = startRedpanda(ctx)
	})
	require.NoError(t, rpErr)
	return rpBrokers
}

func startRedpanda(ctx context.Context) error {
	ctr, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v24.3.1",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		return fmt.Errorf("testenv: start redpanda: %w", err)
	}
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		_ = ctr.Terminate(context.WithoutCancel(ctx))
		return fmt.Errorf("testenv: redpanda broker: %w", err)
	}
	rpContainer = ctr
	rpBrokers = []string{broker}
	return nil
}

func teardownRedpanda(ctx context.Context) {
	if rpContainer != nil {
		_ = rpContainer.Terminate(ctx)
		rpContainer = nil
	}
}
