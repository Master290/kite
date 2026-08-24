#!/bin/sh
set -eu

volume="${KITE_DATA_VOLUME:-kite_kite-data}"
output="${1:-kite-data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz}"

case "$output" in
  /*) ;;
  *) output="$(pwd)/$output" ;;
esac

umask 077
mkdir -p "$(dirname "$output")"
docker run --rm \
  -v "${volume}:/data:ro" \
  -v "$(dirname "$output"):/backup" \
  alpine:3.23 \
  tar czf "/backup/$(basename "$output")" -C /data .
printf 'Created protected backup: %s\n' "$output"
