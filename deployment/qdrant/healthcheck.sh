#!/bin/bash
# The official image contains Bash, but does not promise curl or wget.
set -eu
exec 3<>/dev/tcp/127.0.0.1/6333
printf 'GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
IFS= read -r status <&3
case "$status" in
    'HTTP/1.0 200 '*|'HTTP/1.1 200 '*) exit 0 ;;
    *) exit 1 ;;
esac
