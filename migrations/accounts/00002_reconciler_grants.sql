-- +goose Up
-- +goose StatementBegin

-- Reconciler role (created in deploy/postgres/init.sql) may SELECT the ledger
-- and INSERT reconciliation reports only. Role may be absent in unit test DBs.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reconciler') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO reconciler;
        GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO reconciler;
        GRANT INSERT, UPDATE ON reconciliation_runs TO reconciler;
        GRANT INSERT ON reconciliation_discrepancies TO reconciler;
        GRANT USAGE, SELECT ON SEQUENCE reconciliation_discrepancies_id_seq TO reconciler;
    END IF;
END $$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reconciler') THEN
        REVOKE INSERT, UPDATE ON reconciliation_runs FROM reconciler;
        REVOKE INSERT ON reconciliation_discrepancies FROM reconciler;
        REVOKE USAGE, SELECT ON SEQUENCE reconciliation_discrepancies_id_seq FROM reconciler;
    END IF;
END $$;
-- +goose StatementEnd
