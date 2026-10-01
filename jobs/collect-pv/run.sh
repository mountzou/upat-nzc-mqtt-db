#!/usr/bin/env bash
# Install as /usr/local/sbin/upat-collect-pv after the deployment review.
# Every collection uses the SAME host ledger. No credentials are sourced as code.
set -euo pipefail
umask 077

fail() { echo "collect-pv launcher: $*" >&2; exit 1; }
[[ $(id -u) == 0 ]] || fail 'must run as root to read the private environment file'
mode=${1:-}
[[ $# -gt 0 ]] && shift
case "$mode" in
  run|initialize|status) ;;
  *) fail 'usage: upat-collect-pv run|initialize|status' ;;
esac
[[ $# == 0 ]] || fail 'this command takes no additional arguments'
env_file=/etc/upat-nzc/pv-ingestor.env
state_dir=/var/lib/upat-nzc/pv-api-accounts/43b12e1f10461e19a7eff30abfdea4ba4ddb1eb2357441b26a75f8a07819cc27
[[ -f "$env_file" && ! -L "$env_file" ]] || fail 'private environment file missing or symlinked'
[[ $(stat -c '%u:%a' "$env_file") == 0:600 ]] || fail 'environment file must be root-owned with mode 0600'
[[ -d "$state_dir" && ! -L "$state_dir" ]] || fail 'shared state directory must already exist'
# Only the image reference is extracted. Docker reads credentials directly
# from the env file; values in that file must use Docker env-file syntax.
image=$(sed -n 's/^PV_INGESTOR_IMAGE=//p' "$env_file")
[[ "$image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]] || fail 'a locally validated image digest is required'

network=upat-nzc-mqtt-db_default
mount="type=bind,source=$state_dir,target=/state"
entrypoint=()
name="upat-collect-pv-${mode}-$$"
if [[ "$mode" == initialize || "$mode" == status ]]; then
  network=none
  entrypoint=(--entrypoint=python)
  [[ "$mode" == status ]] && mount+=,readonly
  command=(api_limits.py "$mode" --state-dir=/state)
else
  [[ -f "$state_dir/api-control.sqlite3" && -f "$state_dir/api-control.lock" ]] || fail 'shared ledger has not been initialized'
  name="upat-collect-pv-${INVOCATION_ID:-$$}"
  command=()
fi

exec /usr/bin/docker run --rm --name="$name" --pull=never --init \
  --network="$network" --read-only \
  --tmpfs=/tmp:rw,noexec,nosuid,nodev,size=32m \
  --user=65534:65534 --cap-drop=ALL --security-opt=no-new-privileges:true \
  --memory=384m --memory-swap=384m --cpus=0.50 --pids-limit=128 --stop-timeout=30 \
  --mount="$mount" --env-file="$env_file" \
  --env=PV_API_STATE_DIR=/state \
  --env=POSTGRES_CONNECT_TIMEOUT_SECONDS=10 --env=PYTHONDONTWRITEBYTECODE=1 \
  "${entrypoint[@]}" "$image" "${command[@]}"
