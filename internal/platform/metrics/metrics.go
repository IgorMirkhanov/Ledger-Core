// Package metrics holds process-wide Prometheus collectors shared by services.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WorkerErrors counts background worker iterations that failed.
// A single collector is shared so accounts and transfers workers can both
// register labels without colliding when linked into the same binary (tests).
var WorkerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "worker_errors_total",
	Help: "Background worker iterations that failed.",
}, []string{"worker"})
