package gateway

import (
	"net/http"
	"strings"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/idempotency"
)

// RequireIdempotencyKey rejects POST without a valid Idempotency-Key (1..128 printable ASCII).
func RequireIdempotencyKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path == "/v1/dev/token" {
			next.ServeHTTP(w, r)
			return
		}
		key := r.Header.Get("Idempotency-Key")
		if err := idempotency.ValidateKey(key); err != nil || !isPrintableASCII(key) {
			writeProblem(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_MISSING", "Idempotency-Key is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// MaxBody limits the request body and requires application/json for POST.
func MaxBody(maxBytes int64) func(http.Handler) http.Handler {
	if maxBytes <= 0 {
		maxBytes = 64 << 10
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				ct := r.Header.Get("Content-Type")
				if !strings.HasPrefix(ct, "application/json") {
					writeProblem(w, r, http.StatusBadRequest, "VALIDATION_FAILED", "Content-Type must be application/json")
					return
				}
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}
