#!/bin/sh
set -eu
# Only committed sources enter either image. This script never contacts the VPS.
repo=$(git rev-parse --show-toplevel)
revision=$(git -C "$repo" rev-parse --verify "${1:-HEAD}^{commit}")
short=$(printf '%s' "$revision" | cut -c 1-12)
context=$(mktemp -d /private/tmp/shelly-compact-build.XXXXXX)
trap 'rm -rf "$context"' EXIT HUP INT TERM
git -C "$repo" archive "$revision" | tar -xf - -C "$context"
for service in api ingestor; do
  case "$service" in
    api) baseline=sha256:d27069f42d0f4cb647d8011a19dfbf81a7dcb71a140b493f033ed847eb788856 ;;
    ingestor) baseline=sha256:540f7d5f5d4e277593a3fea2e9bccd6dc96a3817483fd2fa5d2d63ce39f984dd ;;
  esac
  test "$(docker image inspect --format '{{.Id}}' "$baseline")" = "$baseline"
  alias="codex-shelly-runtime-$service:20260916"
  docker image tag "$baseline" "$alias"
  docker build --platform linux/amd64 --network none --pull=false \
    --build-arg "RUNTIME_IMAGE=$alias" --build-arg "SOURCE_COMMIT=$revision" \
    -f "$context/ops/shelly-compact/Dockerfile.$service" \
    -t "codex-shelly-compact-$service:$short" "$context"
  docker image inspect --format '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}' "codex-shelly-compact-$service:$short"
done
