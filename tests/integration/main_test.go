//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/IgorMirkhanov/ledger-core/tests/integration/testenv"
)

func TestMain(m *testing.M) {
	code := 1
	defer func() {
		testenv.Teardown()
		os.Exit(code)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := testenv.SetupPostgres(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "integration: postgres: %v\n", err)
		return
	}
	code = m.Run()
}
