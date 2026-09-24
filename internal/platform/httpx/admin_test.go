package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func readyz(t *testing.T, checks ...ReadinessCheck) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	AdminHandler(checks...).ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestReadyzOptionalDependencyDoesNotFailReadiness(t *testing.T) {
	down := func(context.Context) error { return errors.New("broker unreachable") }
	up := func(context.Context) error { return nil }

	code, body := readyz(t,
		ReadinessCheck{Name: "postgres", Check: up},
		ReadinessCheck{Name: "kafka", Check: down, Optional: true},
	)
	if code != http.StatusOK || !strings.Contains(body, "degraded: broker unreachable") {
		t.Fatalf("optional failure: code=%d body=%s", code, body)
	}

	code, _ = readyz(t, ReadinessCheck{Name: "postgres", Check: down})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("critical failure must be 503, got %d", code)
	}
}
