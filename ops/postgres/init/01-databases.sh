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
# Temporal's persistence: two databases, one role, schema set up by Temporal's
# own tooling at start (RFC 0001 §3.1.1.10).
create_service_db temporal            temporal_role "${TEMPORAL_DB_PASSWORD:?}"
create_service_db temporal_visibility temporal_role "${TEMPORAL_DB_PASSWORD:?}"
