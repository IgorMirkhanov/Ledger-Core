package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
)

func TestMetricsUseRoutePatternNotRawPath(t *testing.T) {
	r := chi.NewRouter()
	r.Use(Metrics)
	r.Get("/v1/accounts/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })

	before := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("/v1/accounts/{id}", http.MethodGet, "418"))
	beforeUnmatched := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("unmatched", http.MethodGet, "404"))

	for _, path := range []string{"/v1/accounts/a", "/v1/accounts/b", "/wp-admin/../etc/passwd", "/random-scan"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		r.ServeHTTP(httptest.NewRecorder(), req)
	}

	if got := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("/v1/accounts/{id}", http.MethodGet, "418")) - before; got != 2 {
		t.Fatalf("route pattern counter = %v, want 2", got)
	}
	if got := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("unmatched", http.MethodGet, "404")) - beforeUnmatched; got != 2 {
		t.Fatalf("unmatched counter = %v, want 2 (raw paths must not become labels)", got)
	}
}
