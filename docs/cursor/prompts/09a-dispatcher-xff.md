# Промпт 09a: Dispatcher без сети в транзакции, XFF из всех строк (исправление по ревью)

**Контекст:** @internal/notifications/dispatcher.go @migrations/notifications @internal/gateway/ratelimit.go @docs/database.md (§3)

---

## 1. Dispatcher отправляет внутри транзакции
Сейчас `tick` открывает транзакцию, берёт до 50 `pending` через `FOR UPDATE SKIP LOCKED` и **внутри неё** вызывает
`Sender.Send` для каждой записи. Проблемы:
- сетевой вызов внутри открытой транзакции нарушает правило проекта (`.cursor/rules/20-money-and-ledger.mdc`,
  `docs/saga.md`): с реальным провайдером (сотни миллисекунд на письмо) транзакция и 50 блокировок живут десятки секунд;
- ошибка UPDATE на любой записи откатывает всю пачку, включая уже отправленные: при следующем тике их отправят **повторно**;
- у ретраев нет паузы: 5 попыток за 5 секунд, и запись уходит в `failed` при минутном сбое провайдера.

**Исправление (та же схема, что с арендой в transfers):**
1. Миграция `migrations/notifications/00002_dispatch_lease.sql`: колонка `next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
   индекс `(next_attempt_at) WHERE status = 'pending'` вместо `notifications_pending_idx`.
2. Захват короткой транзакцией с арендой:
   ```sql
   UPDATE notifications SET next_attempt_at = now() + interval '60 seconds'
   WHERE id IN (SELECT id FROM notifications
                WHERE status = 'pending' AND next_attempt_at <= now()
                ORDER BY next_attempt_at
                LIMIT $1 FOR UPDATE SKIP LOCKED)
   RETURNING id, user_id, channel, template, payload, attempts;
   ```
3. Отправка **вне транзакции**, каждая запись отдельно, с таймаутом 10s на `Send`. После неё отдельный `UPDATE` одной записи:
   успех → `sent`; ошибка → `attempts+1`, `next_attempt_at = now() + backoff(attempts)` (экспонента от 5s до 10 мин),
   после `maxSendAttempts` → `failed`. Ошибка UPDATE одной записи не влияет на остальные.
4. `Sender.Send` получает `n.ID` как ключ идемпотентности провайдера и документирует это: доставка at-least-once
   (упал между Send и UPDATE → повтор после истечения аренды), провайдер дедуплицирует по ключу.
5. Обнови `docs/database.md` §3.

Тесты (integration): Sender падает 2 раза → запись `sent`, `attempts=3`, между попытками соблюдается `next_attempt_at`
(фейковое время через параметр `now` в запросах или короткий backoff в тесте); Sender блокируется → вторая реплика
dispatcher ту же запись не берёт; ошибка одной записи не мешает остальным в пачке.

## 2. `X-Forwarded-For` из нескольких строк
`ClientIP` читает `r.Header.Get("X-Forwarded-For")`, то есть только **первую** строку заголовка. Если прокси добавляет
адрес отдельной строкой, а не дописывает в существующую (так делают некоторые балансировщики), первая строка
целиком от клиента, и подмена IP снова работает.

**Исправление:** `strings.Join(r.Header.Values("X-Forwarded-For"), ",")`, дальше тот же разбор справа налево.
Тест: две строки заголовка `X-Forwarded-For: 6.6.6.6` и `X-Forwarded-For: 203.0.113.7` от доверенного прокси
→ результат `203.0.113.7`.

## Критерии приёмки
`make test && make lint && make test-integration` зелёные.
Коммиты: `fix(notifications): send outside transaction with lease and backoff`, `fix(gateway): read all X-Forwarded-For lines`.
