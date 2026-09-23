-- +goose Up
-- +goose StatementBegin

-- ---------------------------------------------------------------------------
-- Переводы (состояние saga).
--   created → funds_held → completed
--   created → failed                      (бизнес-отказ на HoldFunds)
--   funds_held → compensating → failed    (бизнес-отказ на Capture)
-- next_attempt_at != NULL означает, что шаг нужно (повторно) выполнить не раньше этого времени.
-- ---------------------------------------------------------------------------
CREATE TYPE transfer_status AS ENUM ('created', 'funds_held', 'completed', 'compensating', 'failed');

CREATE TABLE transfers (
    id                UUID            PRIMARY KEY,
    owner_id          UUID            NOT NULL,
    source_account_id UUID            NOT NULL,
    dest_account_id   UUID            NOT NULL,
    amount            BIGINT          NOT NULL CHECK (amount > 0),
    currency          CHAR(3)         NOT NULL,
    dest_amount       BIGINT          NOT NULL CHECK (dest_amount > 0),
    dest_currency     CHAR(3)         NOT NULL,
    fx_rate           NUMERIC(20, 10),
    status            transfer_status NOT NULL DEFAULT 'created',
    hold_id           UUID,
    journal_entry_id  UUID,
    failure_code      TEXT,
    failure_reason    TEXT,
    attempts          INT             NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ,
    version           BIGINT          NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,

    CONSTRAINT transfers_distinct_accounts CHECK (source_account_id <> dest_account_id),
    CONSTRAINT transfers_fx_consistency CHECK (
        (currency = dest_currency AND fx_rate IS NULL AND dest_amount = amount)
        OR (currency <> dest_currency AND fx_rate IS NOT NULL)
    ),
    CONSTRAINT transfers_funds_held_has_hold CHECK (status NOT IN ('funds_held', 'compensating') OR hold_id IS NOT NULL),
    CONSTRAINT transfers_completed_has_entry CHECK (status <> 'completed' OR journal_entry_id IS NOT NULL),
    CONSTRAINT transfers_failed_has_reason   CHECK (status <> 'failed' OR failure_code IS NOT NULL)
);

-- Recovery worker: незавершённые переводы, у которых подошло время следующей попытки.
CREATE INDEX transfers_pending_idx ON transfers (next_attempt_at)
    WHERE status IN ('created', 'funds_held', 'compensating');

-- Список переводов пользователя (keyset по created_at, id).
CREATE INDEX transfers_owner_idx ON transfers (owner_id, created_at DESC, id DESC);

-- ---------------------------------------------------------------------------
-- Журнал шагов saga: аудит и отладка («что происходило с переводом»).
-- ---------------------------------------------------------------------------
CREATE TABLE transfer_steps (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id UUID        NOT NULL REFERENCES transfers (id),
    step        TEXT        NOT NULL,  -- hold | capture | release
    outcome     TEXT        NOT NULL,  -- ok | business_error | transient_error
    error_code  TEXT,
    error       TEXT,
    duration_ms INT         NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX transfer_steps_transfer_idx ON transfer_steps (transfer_id, id);

-- ---------------------------------------------------------------------------
-- FX-курсы. rate: сколько единиц quote за 1 единицу base (в мажорных единицах).
-- Курс фиксируется в переводе на момент создания (transfers.fx_rate).
-- ---------------------------------------------------------------------------
CREATE TABLE fx_rates (
    base       CHAR(3)         NOT NULL,
    quote      CHAR(3)         NOT NULL,
    rate       NUMERIC(20, 10) NOT NULL CHECK (rate > 0),
    updated_at TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (base, quote),
    CONSTRAINT fx_rates_distinct CHECK (base <> quote)
);

INSERT INTO fx_rates (base, quote, rate) VALUES
    ('USD', 'RUB', 90.0000000000), ('RUB', 'USD', 0.0110000000),
    ('EUR', 'RUB', 98.0000000000), ('RUB', 'EUR', 0.0101000000),
    ('EUR', 'USD', 1.0800000000),  ('USD', 'EUR', 0.9200000000),
    ('CNY', 'RUB', 12.5000000000), ('RUB', 'CNY', 0.0790000000);

-- ---------------------------------------------------------------------------
-- Идемпотентность и outbox — та же структура, что в accounts.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    scope         TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    request_hash  BYTEA       NOT NULL,
    response      BYTEA,
    response_code TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '48 hours',

    PRIMARY KEY (scope, key)
);

CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

CREATE TABLE outbox (
    id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    topic          TEXT        NOT NULL,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    headers        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS fx_rates;
DROP TABLE IF EXISTS transfer_steps;
DROP TABLE IF EXISTS transfers;
DROP TYPE  IF EXISTS transfer_status;
-- +goose StatementEnd
