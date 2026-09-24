// Package httpx contains HTTP servers: the admin server (health, metrics) and helpers for the public API.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessCheck returns nil when a dependency is healthy.
//
// Only hard dependencies may make a pod unready: an unready pod is removed from load balancing,
// so a check on a dependency the service can work without turns that dependency's outage into
// a full outage. Mark such dependencies Optional: a failure is reported as "degraded: ..."
// in the /readyz body but the status stays 200.
type ReadinessCheck struct {
	Name     string
	Check    func(ctx context.Context) error
	Optional bool
}

// Server wraps http.Server as an app.Runner with graceful shutdown.
type Server struct {
	name string
	srv  *http.Server
	log  *slog.Logger
}

func NewServer(name, addr string, h http.Handler, log *slog.Logger) *Server {
	return &Server{
		name: name,
		log:  log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           h,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

func (s *Server) Name() string { return s.name }

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", slog.String("server", s.name), slog.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("%s: %w", s.name, err)
		}
		close(errCh)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	}
}

// AdminHandler serves /healthz (liveness), /readyz (dependencies) and /metrics.
func AdminHandler(checks ...ReadinessCheck) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		result := map[string]string{}
		code := http.StatusOK
		for _, c := range checks {
			if err := c.Check(ctx); err != nil {
				if c.Optional {
					result[c.Name] = "degraded: " + err.Error()
					continue
				}
				result[c.Name] = err.Error()
				code = http.StatusServiceUnavailable
			} else {
				result[c.Name] = "ok"
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}
