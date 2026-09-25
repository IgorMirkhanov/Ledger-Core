# Эксплуатация

Runbook для дежурного. Каждый алерт из `deploy/prometheus/alerts.yml` ссылается на раздел этого документа.
Манифесты: `deploy/k8s`. Готовность к проду и открытые пункты: [production-readiness.md](production-readiness.md).

## Выпуск релиза

Actions → **Release** → Run workflow (ветка `main`), `version` = `vX.Y.Z`. Workflow сам ставит тег на
текущий `main`, собирает и сканирует образы (Trivy), публикует их в GHCR с SBOM и provenance,
подписывает cosign и создаёт GitHub Release с номерами образов. Альтернатива: `git push origin vX.Y.Z`.

```bash
# проверка подписи образа
cosign verify ghcr.io/igormirkhanov/ledger-core-gateway:X.Y.Z \
  --certificate-identity-regexp 'https://github.com/IgorMirkhanov/Ledger-Core/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Выкладка

Порядок важен: миграции только добавляют (docs/database.md), поэтому старая версия приложения
продолжает работать на новой схеме, и откат приложения не требует отката схемы.

```bash
# 0. Один раз на кластер: топики Kafka (в проде KAFKA_AUTO_CREATE_TOPICS=false)
BOOTSTRAP=kafka-0.kafka:9092 ./deploy/kafka/topics.sh

# 1. Секреты из менеджера секретов (пример структуры: deploy/k8s/base/secret.example.yaml)

# 2. Миграции (Job'ы с MIGRATE_ONLY=true, под advisory lock goose)
cd deploy/k8s/migrations && kustomize edit set image \
  ghcr.io/igormirkhanov/ledger-core-accounts:vX.Y.Z \
  ghcr.io/igormirkhanov/ledger-core-transfers:vX.Y.Z \
  ghcr.io/igormirkhanov/ledger-core-notifications:vX.Y.Z && cd -
kubectl delete job -n ledger -l app.kubernetes.io/part-of=ledger-core --ignore-not-found
kubectl apply -k deploy/k8s/migrations
kubectl -n ledger wait --for=condition=complete job --all --timeout=10m

# 3. Приложение (теги образов в overlays/prod/kustomization.yaml)
kubectl apply -k deploy/k8s/overlays/prod
kubectl -n ledger rollout status deploy/accounts deploy/transfers deploy/notifications deploy/gateway
```

**Откат:** `kubectl -n ledger rollout undo deploy/<name>`. Схему не откатываем: следующая миграция
исправляет предыдущую (правило проекта).

**Проверка после выкладки:** `/readyz` всех подов 200; в Grafana (дашборд Ledger Core) нет роста 5xx;
`kubectl -n ledger create job --from=cronjob/reconciler reconcile-now` завершился успешно.

## Проверки здоровья

| Эндпоинт | Смысл |
|----------|-------|
| `/healthz` | процесс жив (liveness) |
| `/readyz` | 503 только если недоступна **своя** база. Kafka, Redis и соседние сервисы помечаются `degraded` в теле ответа, но под остаётся в балансировке: сервис умеет работать без них |

<a id="reconciliation-discrepancies"></a>
## Расхождения сверки (critical)

Алерты: `LedgerReconciliationDiscrepancies`, `LedgerReconciliationStale`.

Сверка нашла нарушение инварианта G1/G2/G3/G9. **Деньги не трогать вручную.**

```sql
-- последний прогон и найденные расхождения (БД accounts)
SELECT * FROM reconciliation_runs ORDER BY started_at DESC LIMIT 5;
SELECT check_name, subject_id, expected, actual
FROM reconciliation_discrepancies WHERE run_id = '<run_id>';
```

| Проверка | Что значит | Первое действие |
|----------|-----------|-----------------|
| `g1_journal_balanced` | проводки записи не сходятся в ноль | невозможно при включённых триггерах: проверить, не отключали ли `ALTER TABLE ... DISABLE TRIGGER` |
| `g2_currency_zero_sum` | сумма балансов по валюте ≠ 0 | найти счёт через `g9` за тот же прогон |
| `g9_balance_equals_postings` | `balance` ≠ сумме проводок | сравнить `balance` и `balance_after` последней проводки; искать прямые `UPDATE accounts` в аудите БД |
| `g3_held_equals_active_holds` | `held` ≠ сумме активных холдов | проверить холды счёта и работу hold expirer (`worker_errors_total{worker="hold-expirer"}`) |
| `expired_active_holds` | активные холды просрочены > 5 минут | expirer не работает: логи accounts, `BackgroundWorkerErrors` |

Исправление только компенсирующей записью (`kind=reversal`) после разбора, никогда `UPDATE` баланса.
`LedgerReconciliationStale` значит, что CronJob не отработал 26 часов: `kubectl -n ledger get jobs`.

<a id="manual-review-transfers"></a>
## Переводы на ручном разборе (critical)

Алерт: `SagaManualReview`. Saga пыталась снять холд, который уже `captured`. Это баг, а не сбой сети.

```sql
-- БД transfers
SELECT id, status, failure_code, hold_id, journal_entry_id, attempts, updated_at
FROM transfers WHERE failure_code = 'MANUAL_REVIEW' ORDER BY updated_at DESC;
SELECT * FROM transfer_steps WHERE transfer_id = '<id>' ORDER BY id;
-- БД accounts
SELECT id, status, journal_entry_id FROM holds WHERE id = '<hold_id>';
```

Если холд `captured` и `journal_entry_id` есть, деньги **уже переведены**: перевод фактически успешен,
статус `failed` неверен. Сообщить клиенту и исправить статус вручную по согласованию, сохранить разбор
в тикете. Причину искать в `transfer_steps`: какой шаг вернул бизнес-ошибку после успешного capture.

<a id="elevated-5xx"></a>
## Рост 5xx на публичном API (critical)

Алерт: `TransfersErrorBudgetBurn` (SLO 99.9% успешных `POST /v1/transfers`).

1. Панели «API 5xx ratio» и «API RPS by route»: какой код. 503 означает, что upstream недоступен или
   открыт circuit breaker; 504 — таймаут upstream; 500 — ошибка в коде (логи gateway по `request_id`).
2. `/readyz` accounts и transfers: если своя БД недоступна, проблема в Postgres.
3. Трейс запроса в Jaeger по `trace_id` из лога gateway покажет медленный или падающий участок.

<a id="high-latency"></a>
## Высокая задержка (warning)

Алерт: `TransfersLatencyHigh` (p99 > 500 мс 10 минут).
- CPU подов (HPA по CPU 70%): упёрлись в `maxReplicas`? Базовая ёмкость: `docs/benchmarks.md`.
- Горячий счёт: многие переводы на один счёт сериализуются на `SELECT … FOR UPDATE` (ADR-0003).
  Видно по росту `rpc_duration_seconds` у `CaptureHold` при нормальной загрузке CPU.
- Пул соединений Postgres (`POSTGRES_MAX_CONNS`, 20 на под): при `replicas × 20` больше `max_connections`
  нужен PgBouncer.

<a id="stuck-transfers"></a>
## Переводы не доходят до финального статуса (warning)

Алерт: `TransfersStuckNonTerminal`.

```sql
SELECT status, count(*), min(updated_at) FROM transfers
WHERE status IN ('created','funds_held','compensating') GROUP BY status;
```

Recovery worker подбирает переводы с `next_attempt_at <= now()`. Если они копятся:
`worker_errors_total{worker="transfer-recovery"}`, доступность accounts (circuit breaker в логах transfers).
Холды страхуются TTL 15 минут: деньги клиента вернутся, даже если saga стоит.

<a id="outbox-backlog"></a>
## Очередь outbox растёт (warning)

Алерты: `OutboxBacklogGrowing`, `OutboxPublishErrors`. Бизнес-операции продолжают работать: события
копятся в таблице `outbox` и уйдут, когда Kafka станет доступна (проверено chaos-сценарием `kafka_down.sh`).

- Kafka доступна? `outbox.last_error` покажет причину: `SELECT last_error, count(*) FROM outbox WHERE published_at IS NULL GROUP BY 1`.
- Relay один на сервис (advisory lock): в логах пода-лидера `became outbox leader`.
- Топика нет, а автосоздание выключено → создать топики (`deploy/kafka/topics.sh`).

<a id="dlq"></a>
## События в DLQ (warning)

Алерт: `NotificationsDLQ`. В заголовках сообщения есть `x-original-topic`, `x-original-offset`, `x-error`, `x-attempts`.

```bash
rpk topic consume ledger.notifications.dlq -X brokers=$BOOTSTRAP -n 20 -f '%h %v\n'
```

После исправления причины события можно вернуть в исходный топик: повторная доставка безопасна,
потребитель дедуплицирует по `event_id` (таблица `processed_events`).

<a id="worker-errors"></a>
## Ошибки фоновых воркеров (warning)

Алерт: `BackgroundWorkerErrors`. Воркеры (recovery, hold-expirer, janitor, notification-dispatcher)
не роняют процесс, а пишут ошибку и повторяют на следующем тике. По метке `worker` найти логи с тем же
`component`. Для hold-expirer «ядовитый» холд логируется с `hold_id` и пропускается, остальные истекают.

<a id="rate-limiter-fail-open"></a>
## Rate limiter отключился (warning)

Алерт: `RateLimiterFailOpen`. Redis недоступен, gateway пропускает запросы без лимитов (fail-open,
осознанный выбор: доступность API важнее лимита). Восстановить Redis; при атаке включить лимиты на
ingress-контроллере.

## Регулярные операции

| Операция | Как |
|----------|-----|
| Сверка вне расписания | `kubectl -n ledger create job --from=cronjob/reconciler reconcile-$(date +%s)` |
| Ротация `JWT_SECRET` | сейчас без двух ключей: смена секрета завершает все сессии. План: поддержка `kid` и двух ключей (production-readiness.md) |
| Резервные копии | PITR на стороне Postgres (WAL-архив), проверка восстановления раз в квартал |
| Очистка | janitor чистит `idempotency_keys` и опубликованный `outbox` сам (`OUTBOX_RETENTION`) |
