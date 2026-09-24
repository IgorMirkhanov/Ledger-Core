package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RPCRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_requests_total",
		Help: "gRPC unary requests.",
	}, []string{"service", "method", "code"})

	RPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_duration_seconds",
		Help:    "gRPC unary latency.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"service", "method"})

	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by route pattern.",
	}, []string{"route", "method", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"route", "method"})

	OutboxPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Outbox rows published to Kafka.",
	}, []string{"service"})

	OutboxPublishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_publish_errors_total",
		Help: "Outbox publish failures.",
	}, []string{"service"})

	OutboxPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_pending_events",
		Help: "Unpublished outbox rows.",
	}, []string{"service"})

	OutboxPublishLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "outbox_publish_lag_seconds",
		Help:    "Age of outbox row at publish time.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60, 300},
	}, []string{"service"})

	IdempotencyReplays = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "idempotency_replays_total",
		Help: "Idempotent request replays.",
	}, []string{"service", "scope"})

	KafkaConsumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_total",
		Help: "Kafka consumer handler outcomes.",
	}, []string{"topic", "result"})
)
