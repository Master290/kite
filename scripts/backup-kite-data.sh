#!/bin/sh
set -eu

volume="${KITE_DATA_VOLUME:-kite_kite-data}"
output="${1:-kite-data-$(date -u +%Y%m%dT%H%M%S).tar.gz}"

umask 077
# Stream the volume through stdout so no host bind mounts are needed;
# this works identically on Linux, macOS, and Git Bash on Windows.
docker run --rm -v "${volume}:/data:ro" alpine:3.23 tar czf - -C /data . > "$output"
printf 'Created protected backup: %s\n' "$output"
