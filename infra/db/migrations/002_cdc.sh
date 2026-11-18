#!/bin/sh
set -eu
STOKORA_CDC_PASSWORD=$(cat /run/secrets/cdc_password)
export STOKORA_CDC_PASSWORD
psql --no-psqlrc -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv cdc_password STOKORA_CDC_PASSWORD
BEGIN;
CREATE ROLE stokora_cdc LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD :'cdc_password';
CREATE ROLE stokora_noaccess NOLOGIN NOSUPERUSER NOREPLICATION;
GRANT CONNECT ON DATABASE stokora TO stokora_cdc;
GRANT USAGE ON SCHEMA operational TO stokora_cdc, stokora_noaccess;
GRANT SELECT ON operational.sales_events TO stokora_cdc;
CREATE PUBLICATION stokora_sales FOR TABLE operational.sales_events;
COMMIT;
-- The slot is created by Debezium and retained across connector restarts.
SQL
