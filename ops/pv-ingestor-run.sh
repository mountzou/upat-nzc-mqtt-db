#!/usr/bin/env bash
# Install as /usr/local/sbin/upat-pv-ingestor after the deployment review.
# Every live mode uses the SAME host ledger. No credentials are sourced as code.
set -euo pipefail
umask 077

fail() { echo "pv-ingestor launcher: $*" >&2; exit 1; }
[[ $(id -u) == 0 ]] || fail 'must run as root to read the private environment file'
mode=${1:-}
[[ $# -gt 0 ]] && shift
case "$mode" in
  scheduled|manual|backfill|initialize|status) ;;
  *) fail 'usage: upat-pv-ingestor scheduled|manual|backfill|initialize|status [options]' ;;
esac
env_file=/etc/upat-nzc/pv-ingestor.env
state_dir=/var/lib/upat-nzc/pv-api
[[ -f "$env_file" && ! -L "$env_file" ]] || fail 'private environment file missing or symlinked'
[[ $(stat -c '%u:%a' "$env_file") == 0:600 ]] || fail 'environment file must be root-owned with mode 0600'
[[ -d "$state_dir" && ! -L "$state_dir" ]] || fail 'shared state directory must already exist'
# Only these two non-secret values are extracted. Docker reads all credentials
# directly from the env file; values in that file must use Docker env-file syntax.
image=$(sed -n 's/^PV_INGESTOR_IMAGE=//p' "$env_file")
version=$(sed -n 's/^PV_INGESTOR_CODE_VERSION=//p' "$env_file")
[[ "$image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]] || fail 'a locally validated image digest is required'
[[ "$version" =~ ^([a-f0-9]{7,40}|sha256:[a-f0-9]{64})$ ]] || fail 'a verified Git revision or source SHA-256 is required'

extra=()
save=--no-save-to-db
if [[ "$mode" == manual || "$mode" == backfill ]]; then
  for arg in "$@"; do
    case "$arg" in
      --target-date=????-??-??|--lookback-days=[1-3]|--skip-meter) extra+=("$arg") ;;
      --save-to-db|--no-save-to-db) save=$arg ;;
      *) fail 'unsupported option; use --target-date=YYYY-MM-DD, --lookback-days=1..3, --skip-meter, --save-to-db or --no-save-to-db' ;;
    esac
  done
else
  [[ $# == 0 ]] || fail 'this mode takes no additional options'
fi

network=upat-nzc-mqtt-db_default
mount="type=bind,source=$state_dir,target=/state"
entrypoint=()
name="upat-pv-ingestor-${mode}-$$"
if [[ "$mode" == initialize || "$mode" == status ]]; then
  network=none
  entrypoint=(--entrypoint=python)
  [[ "$mode" == status ]] && mount+=,readonly
  command=(api_control.py "$mode" --state-dir=/state)
else
  [[ -f "$state_dir/api-control.sqlite3" && -f "$state_dir/api-control.lock" ]] || fail 'shared ledger has not been initialized'
  if [[ "$mode" == scheduled ]]; then
    name=upat-pv-ingestor-scheduled
    save=--save-to-db
  fi
  command=(--live --lookback-days=3 "$save" --site-key=upat-pv "${extra[@]}" "--trigger-kind=$mode")
fi

exec /usr/bin/docker run --rm --name="$name" --pull=never --init \
  --network="$network" --read-only \
  --tmpfs=/tmp:rw,noexec,nosuid,nodev,size=32m \
  --user=65534:65534 --cap-drop=ALL --security-opt=no-new-privileges:true \
  --memory=384m --memory-swap=384m --cpus=0.50 --pids-limit=128 --stop-timeout=30 \
  --mount="$mount" --env-file="$env_file" \
  --env=PV_API_STATE_DIR=/state --env=FUSIONSOLAR_INCLUDE_METER=true \
  --env=POSTGRES_CONNECT_TIMEOUT_SECONDS=10 --env=PYTHONDONTWRITEBYTECODE=1 \
  "${entrypoint[@]}" "$image" "${command[@]}"
