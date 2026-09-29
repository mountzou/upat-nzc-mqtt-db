#!/bin/bash
# Run ONLY against a cleanly stopped cluster; destination must be freshly empty.
# The caller must first verify exact source/destination mounts, IDs and paused writers.
set -Eeuo pipefail
export LC_ALL=C
[[ $# = 4 ]] || { echo 'source destination expected-cluster expected-destination-fsid' >&2; exit 64; }
src=$1; dst=$2; expected_cluster=$3; expected_fsid=$4
[[ -d "$src" && ! -L "$src" && -f "$src/PG_VERSION" && ! -e "$src/postmaster.pid" ]]
[[ -d "$dst" && ! -L "$dst" && -z "$(find "$dst" -mindepth 1 -print -quit)" ]]
[[ "$(readlink -f "$src")" != "$(readlink -f "$dst")" ]]
[[ "$(stat -f -c %i "$dst")" = "$expected_fsid" ]]
[[ -z "$(find "$src/pg_tblspc" -mindepth 1 -maxdepth 1 -print -quit)" ]]
[[ ! -L "$src/pg_wal" ]]
control=$(pg_controldata "$src")
[[ "$(sed -n 's/^Database system identifier: *//p' <<<"$control")" = "$expected_cluster" ]]
[[ "$(sed -n 's/^Database cluster state: *//p' <<<"$control")" = 'shut down' ]]
rsync -aHAX --numeric-ids -- "$src/" "$dst/"
sync -f "$dst"
# --delete is deliberately paired with --dry-run: detect extras without deleting anything.
differences=$(rsync -aHAX --numeric-ids --checksum --dry-run --delete --itemize-changes -- "$src/" "$dst/")
[[ -z "$differences" ]] || { printf '%s\n' "$differences" >&2; exit 1; }
echo 'COPY_VERIFIED: every file checksum and metadata match; source untouched'
