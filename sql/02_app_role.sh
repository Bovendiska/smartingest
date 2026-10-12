#!/bin/bash
# Creates the least-privilege role the API connects with.
# The superuser (POSTGRES_USER) is only for administration and migrations.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v app_password="$APP_DB_PASSWORD" <<'SQL'
CREATE ROLE smartingest_app LOGIN PASSWORD :'app_password';
REVOKE ALL ON SCHEMA public FROM smartingest_app;

GRANT USAGE ON SCHEMA meta, quarantine TO smartingest_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA meta, quarantine TO smartingest_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA meta TO smartingest_app;

-- The app creates one typed table per source in silver and drops expired demo tables.
GRANT USAGE, CREATE ON SCHEMA silver TO smartingest_app;
SQL
