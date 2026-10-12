#!/bin/bash
# Daily database backup. Run from the project folder; schedule with cron (see README, Production).
# Keeps 14 days locally. Copy the backups folder off the server too: a backup that lives only
# on the same machine disappears with it.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p backups
stamp=$(date +%Y%m%d-%H%M)
docker compose exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' \
  > "backups/smartingest-$stamp.dump"
find backups -name 'smartingest-*.dump' -mtime +14 -delete
echo "Backup written: backups/smartingest-$stamp.dump"
