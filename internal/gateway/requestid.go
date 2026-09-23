package gateway

import (
	"log/slog"
	"net/http"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/httpx"
	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
)

const maxRequestIDLen = 64

// RequestIDMiddleware accepts X-Request-Id (≤64 chars) or generates a UUIDv7.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > maxRequestIDLen || !utf8.ValidString(id) {
			id = uuid.Must(uuid.NewV7()).String()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := withRequestID(r.Context(), id)
		ctx = logger.With(ctx, slog.String("request_id", id))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title string) {
	httpx.WriteProblem(w, httpx.Problem{
		Type:      "https://ledger-core.dev/errors/" + kebab(code),
		Title:     title,
		Status:    status,
		Code:      code,
		RequestID: RequestID(r.Context()),
	})
}

func kebab(code string) string {
	b := make([]byte, 0, len(code)+4)
	for i := 0; i < len(code); i++ {
		c := code[i]
		switch {
		case c == '_':
			b = append(b, '-')
		case c >= 'A' && c <= 'Z':
			b = append(b, c-'A'+'a')
		default:
			b = append(b, c)
		}
	}
	return string(b)
}
