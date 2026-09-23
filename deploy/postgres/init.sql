-- One Postgres instance, one database per service (database-per-service pattern).
-- In production these would be separate clusters.
CREATE DATABASE accounts;
CREATE DATABASE transfers;
CREATE DATABASE notifications;

-- Read-only role for the reconciler.
CREATE ROLE reconciler LOGIN PASSWORD 'reconciler';
\connect accounts
GRANT CONNECT ON DATABASE accounts TO reconciler;
GRANT USAGE ON SCHEMA public TO reconciler;
ALTER DEFAULT PRIVILEGES FOR ROLE ledger IN SCHEMA public GRANT SELECT ON TABLES TO reconciler;
