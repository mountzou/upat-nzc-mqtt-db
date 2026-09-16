#!/bin/bash
# Check the already copied cluster BEFORE the official entrypoint can initialize it.
set -Eeuo pipefail
export LC_ALL=C
fail() { echo "PG_VOLUME_GUARD: $*" >&2; exit 78; }
: "${PGDATA:?PGDATA required}"
: "${PG_EXPECT_FSID:?expected filesystem ID required}"
: "${PG_EXPECT_SYSTEM_ID:?expected cluster ID required}"
: "${PG_EXPECT_MAJOR:?expected major version required}"
[[ "$PGDATA" = /var/lib/postgresql/data ]] || fail 'unexpected PGDATA'
[[ "${1:-}" = postgres ]] || fail 'only postgres startup is allowed'
[[ -d "$PGDATA" && ! -L "$PGDATA" ]] || fail 'missing or symlink data directory'
[[ "$(stat -f -c %i "$PGDATA")" = "$PG_EXPECT_FSID" ]] || fail 'wrong filesystem'
[[ -f "$PGDATA/PG_VERSION" && ! -L "$PGDATA/PG_VERSION" ]] || fail 'missing PG_VERSION; initialization forbidden'
[[ "$(cat "$PGDATA/PG_VERSION")" = "$PG_EXPECT_MAJOR" ]] || fail 'wrong PostgreSQL major'
[[ "$(stat -c %u "$PGDATA")" = "$(id -u postgres)" ]] || fail 'wrong data owner'
[[ "$(stat -c %g "$PGDATA")" = "$(id -g postgres)" ]] || fail 'wrong data group'
[[ "$(stat -c %a "$PGDATA")" =~ ^(700|750)$ ]] || fail 'unsafe data permissions'
for d in base global pg_wal pg_tblspc; do
    [[ -d "$PGDATA/$d" && ! -L "$PGDATA/$d" ]] || fail "missing or external $d"
done
[[ -f "$PGDATA/global/pg_control" && ! -L "$PGDATA/global/pg_control" ]] || fail 'missing control file'
[[ ! -e "$PGDATA/postmaster.pid" ]] || fail 'postmaster.pid present; investigate before restarting'
[[ -z "$(find "$PGDATA/pg_tblspc" -mindepth 1 -maxdepth 1 -print -quit)" ]] || fail 'external tablespaces need a separate plan'
control="$(pg_controldata "$PGDATA")" || fail 'cannot read control file'
cluster="$(sed -n 's/^Database system identifier: *//p' <<<"$control")"
state="$(sed -n 's/^Database cluster state: *//p' <<<"$control")"
[[ "$cluster" = "$PG_EXPECT_SYSTEM_ID" ]] || fail 'wrong cluster'
[[ "$state" = 'shut down' ]] || fail 'unclean cluster; manual recovery review required'
echo 'PG_VOLUME_GUARD: verified existing cluster; initialization is forbidden' >&2
exec /usr/local/bin/docker-entrypoint.sh "$@"
