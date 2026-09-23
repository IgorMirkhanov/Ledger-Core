# Промпт 08: Gateway (REST → gRPC)

**Контекст:** @docs/api.md @internal/platform/httpx @internal/platform/grpcx @api/proto @cmd/gateway/main.go

---

Задача: `internal/gateway/` + wiring в `cmd/gateway/main.go` (закрой TODO). Роутер: `github.com/go-chi/chi/v5`.

## Middleware (в этом порядке)
1. `RequestID`: `X-Request-Id` из запроса (валидировать ≤ 64 символов) или UUIDv7; в ответ и в ctx/логгер.
2. `Recoverer` → 500 problem+json.
3. `AccessLog` (slog): method, route pattern, status, duration, user_id.
4. `Auth` (кроме `/v1/dev/token`): JWT HS256 (`github.com/golang-jwt/jwt/v5`), `sub` = UUID, `exp` обязателен, leeway 30s → user_id в ctx. Ошибка: 401.
5. `RateLimit`: `github.com/go-redis/redis_rate/v10`, ключ `rl:user:<id>` или `rl:ip:<ip>`, лимит `RATE_LIMIT_RPS`/s с burst ×2.
   Redis недоступен → **fail open** (пропустить + метрика + WARN). Заголовки `X-RateLimit-Remaining`, `Retry-After`.
6. `RequireIdempotencyKey` для POST (1..128 печатных ASCII).
7. `MaxBody` 64KB, `Content-Type: application/json` для POST.

## Handlers
Все эндпоинты из `docs/api.md`. Для каждого: decode (DisallowUnknownFields) → валидация → gRPC-вызов с metadata
(`x-owner-id`, `idempotency-key`, `x-request-id`), deadline `UPSTREAM_TIMEOUT` → encode.

**Безопасность metadata (критично).** Accounts считает вызов внутренним, если есть `x-caller: transfers`
и нет `x-owner-id` (ADR-0008, промпт 05). Поэтому:
- исходящий gRPC-контекст gateway собирает **с нуля**: `metadata.NewOutgoingContext(ctx, metadata.Pairs(...))`
  только с перечисленными ключами. Никогда не пробрасывай HTTP-заголовки клиента в metadata и не используй
  `metadata.AppendToOutgoingContext` поверх чужого контекста;
- `x-owner-id` берётся **только** из проверенного JWT и передаётся в **каждом** вызове, включая GET;
- тест: запрос с заголовками `X-Caller: transfers`, `X-Owner-Id: <чужой uuid>`, `Idempotency-Key` →
  фейковый gRPC-сервер получает `x-owner-id` из JWT и не получает `x-caller`.

**Суммы.** В JSON суммы передаются строками. Разбор строгий: регэксп `^[1-9][0-9]{0,18}$`, затем `strconv.ParseInt(s, 10, 64)`.
Отклоняй `"0"`, `"-5"`, `"+5"`, `"1.5"`, `"1e3"`, `" 5"`, числа JSON без кавычек и значения больше int64 → 400 `VALIDATION_FAILED`.
Table-driven тест на все эти случаи. В ответах суммы — `strconv.FormatInt`.
- Ошибки: `grpcToProblem(err)` по таблице маппинга в docs/api.md, `type` = `https://ledger-core.dev/errors/<kebab-code>`.
- Replay: accounts/transfers уже выставляют gRPC header `idempotent-replayed: true` (промпты 05–07).
  Gateway читает его через `grpc.Header(&md)` и ставит HTTP-заголовок `Idempotent-Replayed: true`.
- `POST /v1/transfers`: 201 для терминального статуса, 202 + `Location: /v1/transfers/{id}` для промежуточного.
- `POST /v1/dev/token` только при `APP_ENV=local`: `{ "user_id": optional }` → JWT на 24h.

## Прочее
- gRPC-клиенты с circuit breaker (переиспользуй подход из accountsclient; вынеси общий код в `grpcx.Dial`).
- `/readyz` проверяет Redis и gRPC health обоих сервисов.
- `api/openapi.yaml` (OpenAPI 3.1) по docs/api.md; в CI проверка через `redocly lint` (добавь job).
- `scripts/demo.sh` (curl + jq): получить токен → создать 2 счёта RUB и USD → депозит → перевод USD→RUB → выписка.

## Тесты
- Unit handlers через `httptest` + фейковые gRPC-клиенты (интерфейсы): маппинг ошибок, строки-суммы, 401/400/422/429.
- Middleware: rate limit с miniredis (`github.com/alicebob/miniredis/v2`), fail-open при недоступном Redis.

Коммиты: `feat(gateway): middleware`, `feat(gateway): handlers`, `docs(api): openapi spec`.
