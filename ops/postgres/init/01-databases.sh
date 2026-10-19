#!/usr/bin/env bash
# One database and one role per service (RFC 0001 §3.1.3.1, RFC 0000 R1).
# Each role may CONNECT to its own database only; PUBLIC is revoked so a role
# connecting to another service's database is refused. Idempotent: `make up`
# re-runs it against an existing volume, so databases added by a later
# milestone appear without a reset.
set -euo pipefail

create_service_db() {
  local db="$1" role="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
       -v db="$db" -v role="$role" -v password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'role', :'password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'db', :'role')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'db') \gexec
SELECT format('GRANT CONNECT, CREATE, TEMPORARY ON DATABASE %I TO %I', :'db', :'role') \gexec
SQL
}

create_service_db identity_db  identity_role  "${IDENTITY_DB_PASSWORD:?}"
create_service_db catalog_db   catalog_role   "${CATALOG_DB_PASSWORD:?}"
# Milestone 2
create_service_db order_db     order_role     "${ORDER_DB_PASSWORD:?}"
create_service_db inventory_db inventory_role "${INVENTORY_DB_PASSWORD:?}"
# Milestone 3: notification consumes events only, so no outbox_relay grant.
create_service_db notification_db notification_role "${NOTIFICATION_DB_PASSWORD:?}"
# Milestone 3: assistant (RFC 0009). pgvector is created here as the superuser,
# because assistant_role may not create extensions; migration 0001 is then a no-op.
create_service_db assistant_db assistant_role "${ASSISTANT_DB_PASSWORD:?}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname assistant_db \
     -c 'CREATE EXTENSION IF NOT EXISTS vector'
# Temporal's persistence: two databases, one role, schema set up by Temporal's
# own tooling at start (RFC 0001 §3.1.1.10).
create_service_db temporal            temporal_role "${TEMPORAL_DB_PASSWORD:?}"
create_service_db temporal_visibility temporal_role "${TEMPORAL_DB_PASSWORD:?}"

# outbox-relay (Milestone 3, RFC 0001 PLT-3): one login role that may CONNECT
# to the producing databases only. Its table rights (SELECT, UPDATE
# (published_at), DELETE on outbox) are granted by each service's migration,
# because this script runs before the tables exist.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
     -v password="${OUTBOX_RELAY_DB_PASSWORD:?}" <<'SQL'
SELECT 'CREATE ROLE outbox_relay LOGIN PASSWORD ' || quote_literal(:'password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'outbox_relay') \gexec
SELECT 'ALTER ROLE outbox_relay LOGIN PASSWORD ' || quote_literal(:'password') \gexec
GRANT CONNECT ON DATABASE identity_db, catalog_db, order_db, inventory_db, assistant_db TO outbox_relay;
SQL
