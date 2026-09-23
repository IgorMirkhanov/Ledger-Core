package service

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	transfersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total",
		Help: "Transfers that reached a terminal status.",
	}, []string{"status"})
	transferDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ledger_transfer_duration_seconds",
		Help:    "Time from transfer creation to a terminal status.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 15, 30, 60, 120, 300, 900},
	})
	sagaAnomalies = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ledger_saga_anomalies_total",
		Help: "Release steps that accounts refused.",
	})
)

func recordTerminal(status string, seconds float64) {
	if seconds < 0 {
		seconds = 0
	}
	transfersTotal.WithLabelValues(status).Inc()
	transferDuration.Observe(seconds)
}

func recordAnomaly(transferID, code, reason string) {
	sagaAnomalies.Inc()
	slog.Error("saga release refused",
		slog.String("transfer_id", transferID),
		slog.String("failure_code", code),
		slog.String("error", reason),
	)
}
