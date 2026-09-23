# Публичный REST API (gateway)

Base URL: `http://localhost:8080/v1`. Формат: JSON. Ошибки: `application/problem+json` (RFC 7807).
OpenAPI-спецификация: `api/openapi.yaml` (создаётся в prompt 08).

## Общие правила

| Правило | Детали |
|---------|--------|
| Аутентификация | `Authorization: Bearer <JWT HS256>`, `sub` = user id (UUID). В `local` есть `POST /v1/dev/token` |
| Идемпотентность | Все `POST` **требуют** `Idempotency-Key` (1..128 символов, рекомендуется UUID). Нет ключа: `400 IDEMPOTENCY_KEY_MISSING` |
| Повтор | Тот же ключ и то же тело: тот же ответ + заголовок `Idempotent-Replayed: true`. Тот же ключ и другое тело: `422 IDEMPOTENCY_KEY_REUSED` |
| Суммы | Строка с целым числом минорных единиц: `"amount": "10050"` = 100.50 RUB. Причина: JS теряет точность на int64 |
| Трассировка | `X-Request-Id` принимается или генерируется, возвращается в ответе; `traceparent` поддерживается |
| Rate limit | На пользователя (или IP без токена): `RATE_LIMIT_RPS`. Превышение: `429` + `Retry-After` |
| Таймаут | Deadline на downstream-вызовы: `UPSTREAM_TIMEOUT` (5s) |

## Эндпоинты

| Метод | Путь | gRPC | Успех |
|-------|------|------|-------|
| POST | `/v1/dev/token` | — (только APP_ENV=local) | 200 `{token, user_id}` |
| POST | `/v1/accounts` | Accounts.CreateAccount | 201 |
| GET | `/v1/accounts` | Accounts.ListAccounts | 200 |
| GET | `/v1/accounts/{id}` | Accounts.GetAccount | 200 |
| GET | `/v1/accounts/{id}/statement?limit=&cursor=&from=&to=` | Accounts.GetStatement | 200 |
| POST | `/v1/accounts/{id}/deposits` | Accounts.Deposit | 201 |
| POST | `/v1/accounts/{id}/withdrawals` | Accounts.Withdraw | 201 |
| POST | `/v1/transfers` | Transfers.CreateTransfer | 201 если `completed`/`failed`, 202 если в процессе |
| GET | `/v1/transfers/{id}` | Transfers.GetTransfer | 200 |
| GET | `/v1/transfers?limit=&cursor=` | Transfers.ListTransfers | 200 |

## Примеры

```http
POST /v1/accounts
Authorization: Bearer eyJ...
Idempotency-Key: 5f0c...
Content-Type: application/json

{"currency": "RUB"}
```
```json
201 Created
{"id":"0192...","currency":"RUB","status":"active","balance":"0","held":"0","available":"0","created_at":"..."}
```

```http
POST /v1/transfers
Idempotency-Key: 7a1d...

{"source_account_id":"0192...","dest_account_id":"0193...","amount":"10000","currency":"USD","dest_currency":"RUB"}
```
```json
201 Created
{"id":"0194...","status":"completed","amount":"10000","currency":"USD","dest_amount":"900000","dest_currency":"RUB","fx_rate":"90","created_at":"...","completed_at":"..."}
```

```json
422 Unprocessable Entity
Content-Type: application/problem+json
{"type":"https://ledger-core.dev/errors/insufficient-funds","title":"Insufficient funds","status":422,"code":"INSUFFICIENT_FUNDS","request_id":"..."}
```

## Маппинг ошибок gRPC → HTTP

| gRPC code / reason | HTTP |
|--------------------|------|
| `INVALID_ARGUMENT` / `VALIDATION_FAILED`, `SAME_ACCOUNT`, `IDEMPOTENCY_KEY_MISSING` | 400 |
| `INVALID_ARGUMENT` / `IDEMPOTENCY_KEY_REUSED` | 422 |
| `UNAUTHENTICATED` | 401 |
| `PERMISSION_DENIED` / `NOT_ACCOUNT_OWNER` | 404 (не раскрываем существование чужого счёта) |
| `NOT_FOUND` | 404 |
| `FAILED_PRECONDITION` / `INSUFFICIENT_FUNDS`, `ACCOUNT_NOT_ACTIVE`, `CURRENCY_MISMATCH`, `FX_RATE_NOT_FOUND` | 422 |
| `RESOURCE_EXHAUSTED` | 429 |
| `UNAVAILABLE`, circuit breaker open | 503 + `Retry-After: 1` |
| `DEADLINE_EXCEEDED` | 504 |
| прочее | 500 (без деталей) |

Перевод, отклонённый по бизнес-причине, возвращается как **201 со `status: "failed"`
и `failure_code`**, а не как 4xx: сам ресурс «перевод» создан, он просто неуспешен.
