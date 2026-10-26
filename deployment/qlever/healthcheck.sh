#!/bin/bash
# Probe the native SPARQL API without requiring curl in the runtime image.
set -euo pipefail
exec 3<>"/dev/tcp/127.0.0.1/${QLEVER_PORT}"
printf 'GET /?query=ASK%%20%%7B%%7D HTTP/1.0\r\nHost: localhost\r\nAccept: application/sparql-results+json\r\nConnection: close\r\n\r\n' >&3
response=$(cat <&3)
[[ "$response" == *" 200 "* ]]
[[ "$response" =~ \"boolean\"[[:space:]]*:[[:space:]]*true ]]
