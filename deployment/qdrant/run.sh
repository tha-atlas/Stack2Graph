#!/bin/sh
# Keep this Compose project independent and derive identity from the host user.
set -eu
case "${1:-}" in
    -h|--help)
        printf '%s\n' 'Usage: run.sh [--env-file FILE ...] COMMAND [OPTIONS]' \
            'Run Qdrant Docker Compose; for example: run.sh up -d --wait.' \
            'Public TLS is provided independently by deployment/gateway.' \
            'Default configuration: deployment/qdrant/.env only.' \
            'Explicit --env-file options replace the default; paths are relative to the caller.'
        exit 0
        ;;
esac
STACK2GRAPH_VECTOR_QDRANT_UID=$(id -u)
STACK2GRAPH_VECTOR_QDRANT_GID=$(id -g)
[ "$STACK2GRAPH_VECTOR_QDRANT_UID" -ne 0 ] || { echo 'Run Qdrant Compose as a non-root host user' >&2; exit 1; }
export STACK2GRAPH_VECTOR_QDRANT_UID STACK2GRAPH_VECTOR_QDRANT_GID
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
