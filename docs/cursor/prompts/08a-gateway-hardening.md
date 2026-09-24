# Промпт 08a: Gateway — порядок лимитов, доверие к прокси, мелочи API (исправление по ревью)

**Контекст:** @internal/gateway @docs/api.md @scripts/demo.sh @cmd/gateway/main.go

Ревью 08 прогнано на живом стенде (accounts + transfers + gateway + Postgres + Redis): demo, replay,
чужой счёт, подделка `X-Caller`, строгие суммы, инварианты G2/G9 в порядке. Исправить нужно следующее.

---

## 1. Rate limit стоит после аутентификации
Сейчас `Auth` → `RateLimit`. Запрос с невалидным токеном получает 401 **до** лимитера, поэтому поток запросов
с мусорными токенами не ограничен ничем (а проверка подписи — самая дорогая часть обработки). Лимит по IP
работает только для `/v1/dev/token`.

**Исправление:** два лимитера.
- `RateLimitIP` **до** `Auth`: ключ `rl:ip:<ip>`, лимит `RATE_LIMIT_IP_RPS` (по умолчанию 100/s, burst ×2).
- `RateLimitUser` после `Auth`: как сейчас, ключ `rl:user:<id>`, `RATE_LIMIT_RPS`.
- Оба fail-open с метрикой `gateway_rate_limit_fail_open_total{limiter}`.
- Тест: 300 запросов с невалидным токеном с одного IP → часть получает 429, а не только 401.

## 2. `X-Forwarded-For` принимается от кого угодно
`clientIP` берёт первый адрес из `X-Forwarded-For`, поэтому клиент подставляет случайный IP в каждом запросе
и обходит IP-лимит полностью.

**Исправление:** конфиг `TRUSTED_PROXY_CIDRS` (по умолчанию пусто).
- Пусто → IP только из `RemoteAddr`, заголовки игнорируются.
- Задано и `RemoteAddr` в доверенной сети → идём по `X-Forwarded-For` **справа налево** и берём первый адрес,
  который не входит в доверенные сети. Левые значения клиент контролирует, правые добавили наши прокси.
- Table-driven тест: без доверенных сетей заголовок игнорируется; с доверенной сетью — правильный адрес
  из цепочки `spoofed, real, proxy`; `RemoteAddr` не из доверенной сети → заголовок игнорируется.
- Добавь `TRUSTED_PROXY_CIDRS` в `.env.example` и `docs/api.md` (раздел «Общие правила», строка Rate limit).

## 3. Единообразие JSON
- `kind` в выписке: сейчас `"TRANSFER"`, а статусы счёта и перевода — строчными (`"completed"`). Сделай строчными: `"transfer"`.
- `posting_id`: int64 отдаётся числом, а суммы строками. Отдавай строкой: то же правило про точность в JS.
- Пустые `failure_code` / `failure_reason` / `completed_at` не выводи (omitempty), а не `""`.
- `fx_rate`: убери хвостовые нули (`"90"`, `"0.011"`), пустое значение не выводи.
- Обнови `api/openapi.yaml` и примеры в `docs/api.md`.

## 4. `scripts/demo.sh` переносимость
`uuidgen` есть не везде (например, в минимальных Linux-образах). Функция `new_key()`: `uuidgen`, если есть,
иначе `cat /proc/sys/kernel/random/uuid`, иначе `openssl rand -hex 16`. Проверка зависимостей в начале
(`curl`, `jq`) с понятной ошибкой.

## Критерии приёмки
`make test && make lint && make test-integration` зелёные, `scripts/demo.sh` проходит на `make up`.
Коммиты: `fix(gateway): ip limiter before auth, trusted proxies`, `fix(gateway): consistent json`, `chore: portable demo script`.
