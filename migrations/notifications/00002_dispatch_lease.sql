-- +goose Up
-- +goose StatementBegin

ALTER TABLE notifications
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP INDEX IF EXISTS notifications_pending_idx;
CREATE INDEX notifications_pending_idx ON notifications (next_attempt_at) WHERE status = 'pending';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS notifications_pending_idx;
CREATE INDEX notifications_pending_idx ON notifications (created_at) WHERE status = 'pending';
ALTER TABLE notifications DROP COLUMN IF EXISTS next_attempt_at;
-- +goose StatementEnd
