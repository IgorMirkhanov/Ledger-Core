package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var workerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "worker_errors_total",
	Help: "Background worker iterations that failed.",
}, []string{"worker"})
