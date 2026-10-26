#!/bin/sh
# Keep public TLS independent from the Qdrant and QLever deployments.
set -eu
case "${1:-}" in
    -h|--help)
        printf '%s\n' 'Usage: run.sh [--env-file FILE ...] COMMAND [OPTIONS]' \
            'Run the shared HTTPS gateway; for example: run.sh up -d --wait.' \
            'Qdrant and QLever remain independent loopback HTTP services.' \
            'Default configuration: deployment/gateway/.env only.' \
            'Explicit --env-file options replace the default; paths are relative to the caller.'
        exit 0
        ;;
esac
service_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
use_defaults=true
option_value=false
for argument in "$@"; do
    if [ "$option_value" = true ]; then
        option_value=false
        continue
    fi
    case "$argument" in
        --env-file|--env-file=*) use_defaults=false; break ;;
        --ansi|--file|-f|--parallel|--profile|--progress|--project-directory|--project-name|-p)
            option_value=true
            ;;
        --) break ;;
        -*) ;;
        *) break ;;
    esac
done
if [ "$use_defaults" = true ]; then
    env_file="$service_dir/.env"
    [ -f "$env_file" ] || { echo "Missing $env_file; copy and configure its .env.example" >&2; exit 1; }
    set -- --env-file "$env_file" "$@"
fi
exec docker compose --project-directory "$service_dir" --file "$service_dir/compose.yml" "$@"
