# Готовность к продакшену

Что уже сделано в коде и манифестах, и что должна обеспечить платформа перед первым боевым запуском.
Эксплуатация: [operations.md](operations.md).

## Сделано

### Корректность денег
- Двойная запись, инварианты в домене **и** в Postgres (триггеры, CHECK), append-only журнал.
- Идемпотентность в одной транзакции с операцией, ключи в пространстве имён владельца.
- Transactional outbox, inbox-дедупликация у потребителя, saga с компенсациями и арендой шагов.
- Ежедневная сверка (CronJob, read-only роль, снимок `REPEATABLE READ`), алерт на расхождения.
- Проверено: тест 1000 параллельных переводов, e2e saga, 4 chaos-сценария, ~25 800 переводов под нагрузкой
  с нулём расхождений (docs/benchmarks.md).

### Защита от небезопасной конфигурации
`APP_ENV=prod` отказывается стартовать, если:
- миграции запускаются при старте приложения (должен работать Job `MIGRATE_ONLY=true`);
- включено автосоздание топиков Kafka;
- включён gRPC reflection;
- `JWT_SECRET` короче 32 байт или равен локальному значению;
- rate limit фактически отключён (значения из нагрузочного override).

`/v1/dev/token` работает только при `APP_ENV=local`.

### Надёжность
- Миграции под advisory lock goose: реплики не накатывают одну миграцию параллельно.
- Graceful shutdown: `preStop` 5 с → остановка приёма → дренаж (15 с) → `terminationGracePeriodSeconds: 30`.
- `/readyz` зависит только от своей БД: отказ Kafka, Redis или соседа не выводит под из балансировки.
- gRPC: `round_robin` через headless Service, `MaxConnectionAge` 5 минут для перебалансировки при
  масштабировании, keepalive согласован между клиентом и сервером.
- Фоновые воркеры не роняют процесс при ошибках.
- Circuit breaker, таймауты, ретраи только транзиентных ошибок.

### Kubernetes (`deploy/k8s`, проверено kubeconform для 1.31 в CI)
- Pod Security Standard `restricted`: non-root, read-only rootfs, `drop: ALL`, seccomp.
- Requests/limits, PDB, HPA по CPU, `topologySpreadConstraints`, `RollingUpdate` с `maxUnavailable: 0`.
- NetworkPolicy: default deny; gRPC accounts доступен только transfers и gateway (accounts доверяет
  `x-caller=transfers`, поэтому сетевая изоляция здесь обязательна); gateway принимает только от ingress.
- Секреты не в репозитории: `secret.example.yaml` описывает структуру, у каждого сервиса свой DSN.

### Поставка
- CI: lint, buf, unit с `-race` и порогом покрытия ядра 70%, integration (testcontainers), govulncheck,
  проверка манифестов, unit-тесты алертов (`promtool test rules`), сборка образов.
- Release по тегу `vX.Y.Z`: сканирование образов (Trivy) в отдельной джобе без прав на запись →
  публикация в GHCR с SBOM и provenance → подпись cosign (keyless).
- Dependabot для Go-модулей, GitHub Actions и базовых образов.
- Go toolchain закреплён на патч-версии (go1.26.8), distroless nonroot образы.

### Наблюдаемость
- Метрики RED для HTTP (по шаблону маршрута) и gRPC, метрики saga, outbox, consumer, воркеров, сверки.
- Трейсинг OpenTelemetry сквозь HTTP → gRPC → Postgres → outbox → Kafka → consumer.
- 11 алертов с runbook'ами и unit-тестами, дашборд Grafana на 14 панелей.

## Должна обеспечить платформа

| Пункт | Почему |
|-------|--------|
| Postgres HA (Patroni / managed), PITR, `sslmode=verify-full` | единственная критическая зависимость |
| PgBouncer (transaction pooling) при `replicas × POSTGRES_MAX_CONNS` > `max_connections` | 20 соединений на под |
| Kafka ≥ 3 брокера, `min.insync.replicas=2`, топики из `deploy/kafka/topics.sh` | продюсер пишет с `acks=all` |
| Redis с репликой (Sentinel / managed) | при отказе лимиты fail-open |
| mTLS между сервисами (service mesh) | сейчас изоляция держится на NetworkPolicy; mTLS добавит аутентификацию `x-caller` |
| External Secrets / Vault для `ledger-secrets` | секреты вне Git |
| Ingress с TLS (cert-manager), `TRUSTED_PROXY_CIDRS` = подсеть ingress-контроллера | корректный клиентский IP для лимитов |
| Prometheus Operator / Alertmanager, Pushgateway для reconciler, OTel Collector | метрики, алерты, трейсы |
| Kubernetes ≥ 1.30 | `preStop.sleep` без shell в distroless-образе |

## Открытые пункты (осознанно не сделано)

| Пункт | Риск | План |
|-------|------|------|
| gRPC закреплён на dev-коммите с фиксом GO-2026-6443 | pre-release зависимость | перейти на v1.85.0 после релиза (комментарий в go.mod) |
| Ротация `JWT_SECRET` без двух ключей | смена секрета разлогинивает всех | `kid` в заголовке JWT, набор активных ключей |
| Внешний IdP (OIDC) вместо собственных HS256-токенов | gateway сам выпускает токены только в `local` | проверять RS256/ES256 токены провайдера по JWKS |
| Горячий системный счёт `settlement.<CUR>` | все депозиты валюты сериализуются | шардирование на N суб-счетов (ADR-0003) |
| Партиционирование `postings` | рост таблицы | `PARTITION BY RANGE (created_at)` по месяцам |
| Сверка на мастере | долгая `REPEATABLE READ` транзакция держит vacuum | запускать на реплике (DSN в примере уже указывает на реплику) |
| Сторонние GitHub Actions по тегам, а не по SHA | подмена тега | закрепить SHA (Dependabot будет обновлять) |
