-- +goose Up
-- +goose StatementBegin

-- ---------------------------------------------------------------------------
-- Справочник валют. minor_units = экспонента (RUB=2 → 1 рубль = 100 копеек).
-- ---------------------------------------------------------------------------
CREATE TABLE currencies (
    code        CHAR(3)  PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    minor_units SMALLINT NOT NULL CHECK (minor_units BETWEEN 0 AND 4)
);

INSERT INTO currencies (code, minor_units) VALUES
    ('RUB', 2), ('USD', 2), ('EUR', 2), ('CNY', 2), ('JPY', 0);

-- ---------------------------------------------------------------------------
-- Счета.
--  kind='customer' — счёт клиента, owner_id обязателен, overdraft запрещён.
--  kind='system'   — внутренние счета банка (settlement.*, fx.*, fee.*),
--                    идентифицируются по code, могут уходить в минус.
--  balance — сумма всех проводок по счёту (кэш, проверяется reconciler'ом).
--  held    — сумма активных холдов.
--  available = balance - held.
-- ---------------------------------------------------------------------------
CREATE TYPE account_kind   AS ENUM ('customer', 'system');
CREATE TYPE account_status AS ENUM ('active', 'frozen', 'closed');

CREATE TABLE accounts (
    id              UUID           PRIMARY KEY,
    kind            account_kind   NOT NULL,
    owner_id        UUID,
    code            TEXT           UNIQUE,
    currency        CHAR(3)        NOT NULL REFERENCES currencies (code),
    status          account_status NOT NULL DEFAULT 'active',
    balance         BIGINT         NOT NULL DEFAULT 0,
    held            BIGINT         NOT NULL DEFAULT 0 CHECK (held >= 0),
    allow_overdraft BOOLEAN        NOT NULL DEFAULT FALSE,
    version         BIGINT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT accounts_available_non_negative CHECK (allow_overdraft OR balance - held >= 0),
    CONSTRAINT accounts_customer_has_owner    CHECK (kind <> 'customer' OR owner_id IS NOT NULL),
    CONSTRAINT accounts_system_has_code       CHECK (kind <> 'system'   OR code IS NOT NULL),
    CONSTRAINT accounts_customer_no_overdraft CHECK (kind <> 'customer' OR allow_overdraft = FALSE)
);

CREATE INDEX accounts_owner_idx ON accounts (owner_id, created_at) WHERE owner_id IS NOT NULL;

-- Системные счета на каждую валюту.
INSERT INTO accounts (id, kind, code, currency, allow_overdraft)
SELECT gen_random_uuid(), 'system', prefix || '.' || c.code, c.code, TRUE
FROM currencies c
CROSS JOIN (VALUES ('settlement'), ('fx'), ('fee')) AS p (prefix);

-- ---------------------------------------------------------------------------
-- Журнальные записи: одна бизнес-операция = одна запись.
-- (reference_type, reference_id, kind) уникальны — второй уровень защиты от дублей
-- поверх idempotency_keys (например, capture одного холда дважды невозможен).
-- ---------------------------------------------------------------------------
CREATE TYPE journal_entry_kind AS ENUM ('deposit', 'withdrawal', 'transfer', 'reversal');

CREATE TABLE journal_entries (
    id             UUID               PRIMARY KEY,
    kind           journal_entry_kind NOT NULL,
    reference_type TEXT               NOT NULL,
    reference_id   TEXT               NOT NULL,
    description    TEXT               NOT NULL DEFAULT '',
    metadata       JSONB              NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ        NOT NULL DEFAULT now(),

    CONSTRAINT journal_entries_reference_uniq UNIQUE (reference_type, reference_id, kind)
);

-- ---------------------------------------------------------------------------
-- Проводки. amount со знаком: + увеличивает баланс счёта, − уменьшает.
-- balance_after — баланс счёта после проводки (для выписок без пересчёта).
-- Keyset-пагинация выписки: WHERE account_id = $1 AND id < $cursor ORDER BY id DESC.
-- ---------------------------------------------------------------------------
CREATE TABLE postings (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id      UUID        NOT NULL REFERENCES journal_entries (id),
    account_id    UUID        NOT NULL REFERENCES accounts (id),
    amount        BIGINT      NOT NULL CHECK (amount <> 0),
    currency      CHAR(3)     NOT NULL REFERENCES currencies (code),
    balance_after BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX postings_account_idx ON postings (account_id, id DESC);
CREATE INDEX postings_entry_idx   ON postings (entry_id);

-- ---------------------------------------------------------------------------
-- Инвариант двойной записи (G1): проверяется в конце транзакции (DEFERRED),
-- т.к. проводки одной записи вставляются несколькими INSERT'ами.
-- ---------------------------------------------------------------------------
CREATE FUNCTION postings_check_balanced() RETURNS TRIGGER AS $$
DECLARE
    bad_currency CHAR(3);
    cnt          INT;
BEGIN
    SELECT count(*) INTO cnt FROM postings WHERE entry_id = NEW.entry_id;
    IF cnt < 2 THEN
        RAISE EXCEPTION 'journal entry % has % postings, need at least 2', NEW.entry_id, cnt
            USING ERRCODE = 'check_violation';
    END IF;

    SELECT currency INTO bad_currency
    FROM postings
    WHERE entry_id = NEW.entry_id
    GROUP BY currency
    HAVING sum(amount) <> 0
    LIMIT 1;

    IF bad_currency IS NOT NULL THEN
        RAISE EXCEPTION 'journal entry % is unbalanced in %', NEW.entry_id, bad_currency
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER postings_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION postings_check_balanced();

-- Валюта проводки обязана совпадать с валютой счёта.
CREATE FUNCTION postings_check_currency() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM accounts WHERE id = NEW.account_id AND currency = NEW.currency) THEN
        RAISE EXCEPTION 'posting currency % does not match account %', NEW.currency, NEW.account_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER postings_currency
    BEFORE INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION postings_check_currency();

-- Неизменяемость журнала (G8).
CREATE FUNCTION forbid_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only', TG_TABLE_NAME
        USING ERRCODE = 'insufficient_privilege';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER postings_append_only
    BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

CREATE TRIGGER journal_entries_append_only
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- ---------------------------------------------------------------------------
-- Холды (резервирование средств).
--  active → captured | released | expired (терминальные).
--  reference_id — внешний идентификатор (например, transfer_id), уникален в рамках счёта.
-- ---------------------------------------------------------------------------
CREATE TYPE hold_status AS ENUM ('active', 'captured', 'released', 'expired');

CREATE TABLE holds (
    id               UUID        PRIMARY KEY,
    account_id       UUID        NOT NULL REFERENCES accounts (id),
    amount           BIGINT      NOT NULL CHECK (amount > 0),
    status           hold_status NOT NULL DEFAULT 'active',
    reference_id     TEXT        NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    journal_entry_id UUID        REFERENCES journal_entries (id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT holds_reference_uniq UNIQUE (account_id, reference_id),
    CONSTRAINT holds_captured_has_entry CHECK (status <> 'captured' OR journal_entry_id IS NOT NULL)
);

-- Для hold expirer: найти истёкшие активные холды.
CREATE INDEX holds_active_expiry_idx ON holds (expires_at) WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- Идемпотентность (G4). Запись создаётся в той же транзакции, что и операция:
-- если транзакция откатилась — ключа нет, запрос можно повторить.
-- Конкурентный дубль блокируется на уникальном индексе до COMMIT первого запроса.
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

-- ---------------------------------------------------------------------------
-- Transactional outbox (G5).
-- ---------------------------------------------------------------------------
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

-- ---------------------------------------------------------------------------
-- Результаты сверки.
-- ---------------------------------------------------------------------------
CREATE TABLE reconciliation_runs (
    id            UUID        PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    status        TEXT        NOT NULL CHECK (status IN ('running', 'ok', 'discrepancies', 'error')),
    checks_total  INT         NOT NULL DEFAULT 0,
    discrepancies INT         NOT NULL DEFAULT 0
);

CREATE TABLE reconciliation_discrepancies (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     UUID        NOT NULL REFERENCES reconciliation_runs (id),
    check_name TEXT        NOT NULL,
    subject_id TEXT        NOT NULL,
    expected   TEXT        NOT NULL,
    actual     TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS reconciliation_discrepancies;
DROP TABLE IF EXISTS reconciliation_runs;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS holds;
DROP TYPE  IF EXISTS hold_status;
DROP TABLE IF EXISTS postings;
DROP TABLE IF EXISTS journal_entries;
DROP FUNCTION IF EXISTS postings_check_balanced();
DROP FUNCTION IF EXISTS postings_check_currency();
DROP FUNCTION IF EXISTS forbid_mutation();
DROP TYPE  IF EXISTS journal_entry_kind;
DROP TABLE IF EXISTS accounts;
DROP TYPE  IF EXISTS account_status;
DROP TYPE  IF EXISTS account_kind;
DROP TABLE IF EXISTS currencies;
-- +goose StatementEnd
