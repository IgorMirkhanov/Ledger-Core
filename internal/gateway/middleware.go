package gateway

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/observability"
)

// Recoverer turns panics into 500 problem+json.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				logger.FromContext(r.Context()).Error("panic",
					slog.Any("panic", p),
					slog.String("stack", string(debug.Stack())),
				)
				writeProblem(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// AccessLog logs method, route pattern, status, duration and user_id.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = r.URL.Path
		}
		attrs := []any{
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		}
		if uid, ok := UserID(r.Context()); ok {
			attrs = append(attrs, slog.String("user_id", uid.String()))
		}
		logger.FromContext(r.Context()).Info("http", attrs...)
	})
}

// Metrics records http_requests_total and http_request_duration_seconds by route pattern.
// It runs before Auth and the rate limiters so 401 and 429 are counted too.
// Unmatched paths share one label value: raw URLs would let any scanner explode label cardinality.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := "unmatched"
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			route = rc.RoutePattern()
		}
		observability.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		observability.HTTPDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}
