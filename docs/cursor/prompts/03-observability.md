# Промпт 03: Observability (OpenTelemetry + Prometheus)

**Контекст:** @docs/architecture.md (раздел 9) @internal/platform @cmd

---

Задача: трейсинг сквозь HTTP → gRPC → Postgres → outbox → Kafka → consumer и RED-метрики.
Закрой все `TODO(prompt-03)` в коде.

## Сделай
1. `internal/platform/observability/tracing.go`:
   `InitTracing(ctx, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error)`,
   OTLP gRPC exporter, `ParentBased(TraceIDRatioBased(1.0))`, propagator `TraceContext + Baggage`.
   Пустой endpoint → noop provider. Регистрировать shutdown в `app.OnShutdown`.
2. `internal/platform/observability/metrics.go`: общие метрики (promauto, default registry):
   - `rpc_requests_total{service,method,code}`, `rpc_duration_seconds{service,method}` (buckets 5ms..5s);
   - `http_requests_total{route,method,status}`, `http_request_duration_seconds{route,method}`;
   - `outbox_published_total{service}`, `outbox_publish_errors_total{service}`, `outbox_pending_events{service}` (gauge, обновлять в relay раз в 5s `SELECT count(*) ... WHERE published_at IS NULL`), `outbox_publish_lag_seconds` (histogram now - created_at);
   - `idempotency_replays_total{service,scope}`;
   - `kafka_consumer_messages_total{topic,result}` (ok|retry|dlq), `kafka_consumer_lag` (опционально).
3. gRPC: `otelgrpc.NewServerHandler()` / `NewClientHandler()` через `grpc.StatsHandler`; unary interceptor метрик в `grpcx`.
   Добавь `trace_id` в request-scoped логгер (LoggingInterceptor).
4. pgx: `github.com/exaring/otelpgx` → `pc.ConnConfig.Tracer`.
5. Outbox: в `Writer.Add` инжектить `traceparent` из ctx в headers; relay прокидывает headers в Kafka.
   Consumer: извлекать контекст из headers, начинать span `consume <topic>` с link/parent.
6. HTTP (gateway, пригодится в 08): `otelhttp.NewHandler` + middleware метрик с route pattern (chi `RouteContext`), а не raw path.
7. Подключи всё в `cmd/*/main.go`.
8. `deploy/grafana/dashboards/ledger.json`: панели RPS / p50-p95-p99 / error rate по сервисам,
   transfers по статусам, outbox pending/lag, consumer DLQ rate, reconciliation discrepancies.

## Критерии приёмки
- `make up`, любой запрос → в Jaeger (http://localhost:16686) один трейс: gateway → accounts → postgres → (позже) notifications.
- `curl localhost:8091/metrics | grep rpc_requests_total` показывает метрики.
- Unit-тест: interceptor метрик увеличивает счётчик с правильным `code`.

Коммит: `feat(observability): otel tracing, prometheus metrics, grafana dashboard`.
