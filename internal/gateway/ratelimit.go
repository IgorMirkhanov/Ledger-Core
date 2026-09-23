package gateway

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
)

var rateLimitFailOpen = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gateway_rate_limit_fail_open_total",
	Help: "Rate limit checks that failed open because Redis was unavailable.",
})

// RateLimit limits requests per user (or IP) using Redis. On Redis errors it fails open.
func RateLimit(rdb *redis.Client, rps int) func(http.Handler) http.Handler {
	if rps <= 0 {
		rps = 50
	}
	limiter := redis_rate.NewLimiter(rdb)
	limit := redis_rate.Limit{Rate: rps, Period: time.Second, Burst: rps * 2}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := rateKey(r)
			res, err := limiter.Allow(r.Context(), key, limit)
			if err != nil {
				if r.Context().Err() == nil {
					rateLimitFailOpen.Inc()
					logger.FromContext(r.Context()).Warn("rate limit fail-open", slog.Any("error", err))
				}
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			if res.Allowed == 0 {
				retry := int(res.RetryAfter.Seconds())
				if retry < 1 {
					retry = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retry))
				writeProblem(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "Too many requests")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func rateKey(r *http.Request) string {
	if uid, ok := UserID(r.Context()); ok {
		return "rl:user:" + uid.String()
	}
	return "rl:ip:" + clientIP(r)
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// PingRedis is a readiness check for Redis.
func PingRedis(rdb *redis.Client) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}
}
