#!/bin/sh
# Authenticate over TCP and execute SQL; suppress output containing credentials.
set -eu
PGPASSWORD=$(cat "$POSTGRES_PASSWORD_FILE")
export PGPASSWORD
export PGCONNECT_TIMEOUT=3
if psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" \
    --no-password --no-psqlrc --quiet --tuples-only --no-align \
    --set=ON_ERROR_STOP=1 --command='SELECT 1' >/dev/null 2>&1; then
    exit 0
fi
exit 1
