# ADR-0008: gRPC внутри, REST снаружи

**Статус:** принято

## Решение
- Межсервисно: gRPC (строгие контракты в `api/proto`, buf lint + breaking-check в CI, deadlines, статусы).
- Клиентам: REST/JSON через gateway (проще для веб/мобайла, OpenAPI).
- Ошибки: `google.rpc.ErrorInfo{reason, domain}` → gateway маппит в RFC 7807 `problem+json`.
- Пользователь передаётся в metadata `x-owner-id`; в проде межсервисная аутентификация через mTLS
  (service mesh), в локальном окружении доверенная сеть docker.

## Альтернативы
grpc-gateway (автогенерация REST): меньше кода, но меньше контроля над форматом ошибок,
идемпотентностью и rate limit; ручной gateway полезнее для демонстрации.
