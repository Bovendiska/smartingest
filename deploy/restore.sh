#!/bin/bash
# Restore a backup into the running database. DANGER: replaces current data.
# Usage: deploy/restore.sh backups/smartingest-YYYYMMDD-HHMM.dump
set -euo pipefail
cd "$(dirname "$0")/.."
file="${1:?usage: deploy/restore.sh backups/<file>.dump}"
read -r -p "This replaces the current database with $file. Type RESTORE to continue: " answer
[ "$answer" = "RESTORE" ] || { echo "Cancelled"; exit 1; }
docker compose exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < "$file"
echo "Restored from $file"