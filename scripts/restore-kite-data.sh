#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 BACKUP.tar.gz" >&2
  exit 2
fi

backup="$1"
volume="${KITE_DATA_VOLUME:-kite_kite-data}"
if [ ! -f "$backup" ]; then
  echo "backup not found: $backup" >&2
  exit 1
fi

case "$backup" in
  *.tar.gz|*.tgz) ;;
  *) echo "backup must be .tar.gz or .tgz" >&2; exit 2 ;;
esac

echo "This replaces the contents of Docker volume $volume. Stop Kite first."
printf 'Continue? [y/N] '
read answer
[ "$answer" = "y" ] || [ "$answer" = "Y" ] || exit 1

# Stream the archive through stdin and wipe the volume in one container,
# avoiding host bind mounts so the script behaves the same everywhere.
docker run --rm -i -v "${volume}:/data" alpine:3.23 sh -c '
  find /data -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  tar xzf - -C /data
' < "$backup"
echo "Restored $backup into $volume"
