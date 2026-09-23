package gateway

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Auth validates Bearer JWT HS256 and puts sub into context.
// skipPaths are exact URL paths that bypass authentication.
func Auth(secret []byte, skipPaths ...string) func(http.Handler) http.Handler {
	skip := make(map[string]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := skip[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}
			raw := r.Header.Get("Authorization")
			if !strings.HasPrefix(raw, "Bearer ") {
				writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Missing bearer token")
				return
			}
			token := strings.TrimSpace(strings.TrimPrefix(raw, "Bearer "))
			claims := jwt.MapClaims{}
			parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
				if t.Method != jwt.SigningMethodHS256 {
					return nil, errors.New("unexpected signing method")
				}
				return secret, nil
			}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
				jwt.WithExpirationRequired(),
				jwt.WithLeeway(30*time.Second),
			)
			if err != nil || !parsed.Valid {
				writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid token")
				return
			}
			sub, _ := claims["sub"].(string)
			id, err := uuid.Parse(sub)
			if err != nil {
				writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Invalid subject")
				return
			}
			next.ServeHTTP(w, r.WithContext(withUserID(r.Context(), id)))
		})
	}
}

// IssueToken signs a HS256 JWT for userID lasting 24h.
func IssueToken(secret []byte, userID uuid.UUID, now time.Time) (string, error) {
	claims := jwt.MapClaims{
		"sub": userID.String(),
		"iat": now.Unix(),
		"exp": now.Add(24 * time.Hour).Unix(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}
