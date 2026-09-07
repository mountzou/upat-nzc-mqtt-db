#!/bin/sh
# Scheduled counter writer. The fixed production image and cutover live in Compose.
# The same lock protects both cron and explicit operator invocations.
set -eu
[ "$#" -eq 0 ] || { echo 'energy aggregator: no arguments accepted' >&2; exit 2; }
exec /usr/bin/flock -n /var/lock/energy-aggregator.lock \
  /usr/bin/docker compose \
    --project-directory /opt/upat-nzc-mqtt-db \
    --env-file /opt/upat-nzc-mqtt-db/.env \
    -p upat-nzc-mqtt-db \
    -f /opt/upat-nzc-mqtt-db/docker-compose.prod.yml \
    run --rm --no-deps --pull never -T --interactive=false energy-aggregator
