#!/bin/bash
# Phase 2: a read-only-on-silver role for dbt, the staging + gold schemas it owns,
# and a separate database for Airflow's own metadata.
#
# Runs automatically on a fresh database volume. Safe to run again by hand on an existing one:
#   docker compose exec postgres bash /docker-entrypoint-initdb.d/03_gold_and_airflow.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v db="$POSTGRES_DB" -v dbt_password="$DBT_DB_PASSWORD" -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
-- Roles (created once; the password is re-applied so it always matches .env)
SELECT format('CREATE ROLE smartingest_dbt LOGIN PASSWORD %L', :'dbt_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'smartingest_dbt') \gexec
ALTER ROLE smartingest_dbt PASSWORD :'dbt_password';

SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'airflow_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') \gexec
ALTER ROLE airflow PASSWORD :'airflow_password';

-- Airflow keeps its metadata in its own database and cannot see SmartIngest's.
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') \gexec
REVOKE CONNECT ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO smartingest_app, smartingest_dbt;

-- dbt writes only to the schemas it owns.
REVOKE ALL ON SCHEMA public FROM smartingest_dbt;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION smartingest_dbt;
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION smartingest_dbt;

-- dbt reads silver and the parts of meta / quarantine it needs, nothing else
-- (no access to meta.demo_sessions, meta.llm_calls or the schema registry).
GRANT USAGE ON SCHEMA silver, meta, quarantine TO smartingest_dbt;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO smartingest_dbt;
GRANT SELECT ON meta.batches, meta.issues, meta.column_profiles, quarantine.rows TO smartingest_dbt;
-- Silver tables are created later by the API (as smartingest_app); grant SELECT on those automatically.
ALTER DEFAULT PRIVILEGES FOR ROLE smartingest_app IN SCHEMA silver GRANT SELECT ON TABLES TO smartingest_dbt;
SQL
