-- +goose Up
-- +goose StatementBegin

-- ---------------------------------------------------------------------------
-- Inbox (G6): дедупликация входящих событий.
-- Вставка в processed_events и создание уведомления происходят в одной транзакции.
-- ---------------------------------------------------------------------------
CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    topic        TEXT        NOT NULL,
    partition    INT         NOT NULL,
    "offset"     BIGINT      NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);

CREATE TYPE notification_status AS ENUM ('pending', 'sent', 'failed');

CREATE TABLE notifications (
    id          UUID                PRIMARY KEY,
    user_id     UUID                NOT NULL,
    event_id    UUID                NOT NULL REFERENCES processed_events (event_id),
    channel     TEXT                NOT NULL CHECK (channel IN ('email', 'push', 'sms')),
    template    TEXT                NOT NULL,
    payload     JSONB               NOT NULL,
    status      notification_status NOT NULL DEFAULT 'pending',
    attempts    INT                 NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ
);

CREATE INDEX notifications_user_idx    ON notifications (user_id, created_at DESC);
CREATE INDEX notifications_pending_idx ON notifications (created_at) WHERE status = 'pending';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS notifications;
DROP TYPE  IF EXISTS notification_status;
DROP TABLE IF EXISTS processed_events;
-- +goose StatementEnd
