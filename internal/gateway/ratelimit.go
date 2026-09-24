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

var rateLimitFailOpen = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_rate_limit_fail_open_total",
	Help: "Rate limit checks that failed open because Redis was unavailable.",
}, []string{"limiter"})

// TrustedProxies holds CIDR networks whose RemoteAddr may set X-Forwarded-For.
type TrustedProxies struct {
	nets []*net.IPNet
}

// ParseTrustedProxies parses comma-separated CIDRs. Empty input yields an empty set
// (X-Forwarded-For is ignored).
func ParseTrustedProxies(cidrs string) (*TrustedProxies, error) {
	tp := &TrustedProxies{}
	for _, part := range strings.Split(cidrs, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		_, network, err := net.ParseCIDR(part)
		if err != nil {
			return nil, err
		}
		tp.nets = append(tp.nets, network)
	}
	return tp, nil
}

func (tp *TrustedProxies) contains(ip net.IP) bool {
	if tp == nil || ip == nil {
		return false
	}
	for _, n := range tp.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ClientIP returns the client address. Without trusted proxies, only RemoteAddr is used.
// With a trusted RemoteAddr, X-Forwarded-For is walked right-to-left and the first address
// outside the trusted set is returned (client-controlled left values are ignored).
func ClientIP(r *http.Request, trusted *TrustedProxies) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	remote := net.ParseIP(host)
	if trusted == nil || len(trusted.nets) == 0 || !trusted.contains(remote) {
		return host
	}
	xff := strings.Join(r.Header.Values("X-Forwarded-For"), ",")
	if xff == "" {
		return host
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		cand := strings.TrimSpace(parts[i])
		ip := net.ParseIP(cand)
		if ip == nil {
			continue
		}
		if !trusted.contains(ip) {
			return cand
		}
	}
	return host
}

// RateLimitIP limits by client IP before authentication.
func RateLimitIP(rdb *redis.Client, rps int, trusted *TrustedProxies) func(http.Handler) http.Handler {
	return rateLimit(rdb, rps, "ip", 100, func(r *http.Request) string {
		return "rl:ip:" + ClientIP(r, trusted)
	})
}

// RateLimitUser limits by authenticated user id after Auth.
func RateLimitUser(rdb *redis.Client, rps int) func(http.Handler) http.Handler {
	return rateLimit(rdb, rps, "user", 50, func(r *http.Request) string {
		if uid, ok := UserID(r.Context()); ok {
			return "rl:user:" + uid.String()
		}
		// Should not happen after Auth; fall back to a shared bucket.
		return "rl:user:anonymous"
	})
}

func rateLimit(rdb *redis.Client, rps int, label string, defaultRPS int, keyFn func(*http.Request) string) func(http.Handler) http.Handler {
	if rps <= 0 {
		rps = defaultRPS
	}
	limiter := redis_rate.NewLimiter(rdb)
	limit := redis_rate.Limit{Rate: rps, Period: time.Second, Burst: rps * 2}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFn(r)
			res, err := limiter.Allow(r.Context(), key, limit)
			if err != nil {
				if r.Context().Err() == nil {
					rateLimitFailOpen.WithLabelValues(label).Inc()
					logger.FromContext(r.Context()).Warn("rate limit fail-open",
						slog.String("limiter", label), slog.Any("error", err))
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

// PingRedis is a readiness check for Redis.
func PingRedis(rdb *redis.Client) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	}
}
