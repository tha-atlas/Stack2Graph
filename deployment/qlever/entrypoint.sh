#!/bin/sh
# Compose owns the server; the graph pipeline publishes a relative index selection.
set -eu
case "$QLEVER_IMAGE" in
    *@sha256:*) ;;
    *) echo 'QLever requires an immutable image reference with sha256 digest' >&2; exit 1 ;;
esac
if [ -e /deployment/active-index ]; then
    selection=$(cat /deployment/active-index)
    case "$selection" in
        ''|.*|*[!a-zA-Z0-9_-]*) echo 'Invalid published QLever index selection' >&2; exit 1 ;;
    esac
    index_dir="/indexes/$selection"
else
    index_dir=/deployment/empty
    if [ ! -d "$index_dir" ]; then
        temporary=$(mktemp -d /deployment/.empty-building-XXXXXXXX)
        trap 'rm -rf -- "$temporary"' EXIT HUP INT TERM
        printf '%s\n' '{"num-triples-per-batch":10000,"parser-batch-size":1000}' > "$temporary/kg.settings.json"
        qlever-index -i "$temporary/kg" -f /dev/null -F nq -p false \
            -s "$temporary/kg.settings.json" -m 402653184B -b 134217728B
        test -f "$temporary/kg.index.pso" && test -f "$temporary/kg.index.pos"
        printf '%s\n' "$QLEVER_IMAGE" > "$temporary/image"
        mv -- "$temporary" "$index_dir"
        trap - EXIT HUP INT TERM
    fi
    [ "$(cat "$index_dir/image")" = "$QLEVER_IMAGE" ] || {
        echo 'Empty index image differs from the configured runtime' >&2; exit 1;
    }
fi
test -f "$index_dir/kg.index.pso" && test -f "$index_dir/kg.index.pos"
cd -- "$index_dir"
exec qlever-server -i kg -p "$QLEVER_PORT" -m "${QLEVER_QUERY_RAM_BYTES}B" \
    -c "$((QLEVER_QUERY_RAM_BYTES / 4))B" -e "$((QLEVER_QUERY_RAM_BYTES / 8))B" \
    -s "${QLEVER_TIMEOUT_SECONDS}s"
