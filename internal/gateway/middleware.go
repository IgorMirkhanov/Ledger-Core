package gateway

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
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
