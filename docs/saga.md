# Saga переводов

Оркестрация (не хореография): вся логика перевода живёт в `transfers`,
`accounts` предоставляет атомарные идемпотентные операции над холдами.

## Состояния

```
                 ┌──────────── hold: business error ────────────┐
                 │                                               ▼
  ┌─────────┐  hold ok  ┌────────────┐  capture ok  ┌───────────┐ ┌────────┐
  │ created ├──────────►│ funds_held ├─────────────►│ completed │ │ failed │
  └─────────┘           └─────┬──────┘              └───────────┘ └────────┘
                              │ capture: business error / HOLD_EXPIRED   ▲
                              ▼                                          │
                       ┌──────────────┐          release ok              │
                       │ compensating ├──────────────────────────────────┘
                       └──────────────┘
```

| Статус | Следующий шаг | Идемпотентный ключ шага |
|--------|---------------|-------------------------|
| `created` | `CreateHold(source, amount, ref=transfer_id, ttl=15m)` | `transfer:<id>:hold` |
| `funds_held` | `CaptureHold(hold, dest, dest_amount)` | `transfer:<id>:capture` |
| `compensating` | `ReleaseHold(hold)` | `transfer:<id>:release` |
| `completed`, `failed` | нет (терминальные) | |

Переходы реализованы в `internal/transfers/domain/transfer.go` и покрыты тестами.

## Классификация ошибок от accounts

| Ответ accounts | Тип | Действие |
|----------------|-----|----------|
| OK | успех | переход в следующий статус |
| `FAILED_PRECONDITION`, `NOT_FOUND`, `PERMISSION_DENIED`, `INVALID_ARGUMENT` | **бизнес-отказ** (`BusinessError`) | hold → `failed`; capture → `compensating` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `UNKNOWN`, `ABORTED`, `RESOURCE_EXHAUSTED`, обрыв сети | **транзиентная**, исход неизвестен | статус не меняется, `attempts++`, `next_attempt_at = now + backoff` |

Транзиентную ошибку можно ретраить только потому, что ключ шага детерминирован:
если первый вызов на самом деле выполнился, повтор вернёт тот же ответ (replay).

**Release не может получить бизнес-отказ по смыслу.** Если холд уже `released` или `expired`,
accounts отвечает OK (идемпотентно). Если `captured` — это баг, который нужно логировать
как `ERROR`, выставлять метрику `ledger_saga_anomalies_total` и не ретраить бесконечно
(после 20 попыток перевод помечается для ручного разбора: `failure_code=MANUAL_REVIEW`).

## Алгоритм `Advance(id)`

```
1. tx1: t := Lock(id)                      -- FOR UPDATE, короткая транзакция
        if t.Status.IsTerminal(): return t
        step := t.NextStep()
        Lease(id, now + stepLease)          -- next_attempt_at в будущее, version не меняется
   COMMIT                                  -- НЕ держим блокировку во время сетевого вызова
2. result := accounts.<step>(ctx, t.IdempotencyKey(step), ...)   -- с таймаутом 3s
3. tx2: t2 := Lock(id)
        if t2.Version != t.Version: return t2   -- кто-то (worker) уже продвинул, выходим
        apply transition (OnHoldCreated / OnHoldRejected / OnCaptured / ... / OnTransientError)
        Update(t2) (optimistic by version)
        AppendStep(log)
        outbox.Add(transfer.<status>) если статус изменился
   COMMIT
4. если статус не терминальный и ошибка не транзиентная: goto 1
```

Почему не держим `FOR UPDATE` во время gRPC-вызова: это бы держало соединение с БД
и блокировку строки на время сетевого запроса (до секунд), что убивает пропускную способность.
Параллельный `Advance` того же перевода (API и worker одновременно) безопасен:
оба вызовут accounts с одним и тем же ключом (accounts выполнит один раз), а на шаге 3
второй увидит изменённую версию и выйдет.

## `CreateTransfer`

```
tx: idem.Begin("transfers.CreateTransfer", key)
    replay → вернуть сохранённый transfer_id → Get → ответ (с актуальным статусом!)
    rate := GetRate(src.cur, dst.cur) если валюты разные
    t := domain.NewTransfer(...)
    repo.Create(t); outbox transfer.created; idem.Complete(transfer_id)
COMMIT
loop Advance(t.id) пока не терминальный, есть время в ctx (оставить запас 500ms) и ошибки не транзиентные
return t
```

Перед созданием transfers вызывает `accounts.GetAccountInfo` для source (проверка владельца и валюты)
и для dest (валюта для FX). Dest может принадлежать другому пользователю: это нормально.

## Recovery worker

Каждые `RECOVERY_INTERVAL` (1s): `ClaimPending(now, 50)` с арендой (см. `docs/database.md` §2) → для каждого id
`Advance(id)` с ограничением параллелизма (semaphore на 10). Корректность при нескольких исполнителях
обеспечивают версии; аренда (`stepLease` = 10s, больше таймаута вызова 3s) убирает **дублирующую работу**:
переводы, которые прямо сейчас продвигает API или другая реплика, не берутся. Без аренды под нагрузкой,
когда шаги медленные, каждый вызов в accounts выполнялся бы дважды.

Воркер **никогда не завершает процесс** из-за ошибки БД или accounts: пишет лог и метрику, ждёт следующий тик.
`Run` возвращает управление только при отмене контекста. То же правило действует для всех фоновых воркеров проекта.

## Страховка: TTL холда

Холд живёт 15 минут. Если transfers «потерял» перевод (баг, удалённая строка, недоступность > TTL),
hold expirer в accounts вернёт деньги. Capture истёкшего холда: `HOLD_EXPIRED`, перевод уходит
в `compensating`, release отвечает OK (холд уже expired), перевод получает `failed / HOLD_EXPIRED`.

## Сценарии для тестов (обязательные)

1. Happy path, одна валюта.
2. Happy path, FX (USD → RUB), проверка 4 проводок и G2.
3. Недостаточно средств → `failed/INSUFFICIENT_FUNDS`, холда нет.
4. Dest закрыт (`closed`) → `compensating` → `failed/ACCOUNT_NOT_ACTIVE`, холд `released`, баланс source восстановлен.
   Dest заморожен (`frozen`) → `completed`: заморозка запрещает только списание (ADR-0009).
5. accounts возвращает `UNAVAILABLE` на capture 3 раза, потом OK → `completed`, `attempts=3`.
6. Таймаут capture, но accounts на самом деле выполнил capture → повтор с тем же ключом → `completed`, одна журнальная запись.
7. Двойной `CreateTransfer` с одним ключом параллельно → один перевод.
8. Тот же ключ, другое тело → `INVALID_ARGUMENT / IDEMPOTENCY_KEY_REUSED`.
9. Холд истёк до capture → `failed/HOLD_EXPIRED`.
10. `Advance` одного перевода из 2 горутин одновременно → корректный финал, одна проводка.
