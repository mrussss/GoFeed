#!/usr/bin/env bash
# Run against a disposable gofeed-evidence-* container only.
set -euo pipefail
docker_cli=${DOCKER_CLI:-docker}
action=${1:?provide redis-stop, redis-start, or mysql-lock}
container=${2:?provide the disposable lab container name}
case "$container" in
  gofeed-evidence-*) ;;
  *) printf 'Expected a disposable gofeed-evidence-* container\n' >&2; exit 2 ;;
esac

case "$action" in
  redis-stop|redis-start)
    [[ "$container" == *-redis ]] || exit 2
    "$docker_cli" "${action#redis-}" "$container"
    ;;
  mysql-lock)
    [[ "$container" == *-mysql ]] || exit 2
    "$docker_cli" exec "$container" sh -ec '
      export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
      exec mysql -uroot gofeed_read_test -e "LOCK TABLES videos WRITE; SELECT SLEEP(1.5); UNLOCK TABLES"
    '
    ;;
  *) printf 'Unknown lab fault\n' >&2; exit 2 ;;
esac
