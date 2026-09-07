# Production Compose consolidation

Status (7 September 2026): **activated and verified**. The active VPS API now
records one production Compose file. Only the API container was recreated; its
image and runtime settings are unchanged. PostgreSQL and its mounts, Caddy,
collectors and all other containers were verified unchanged.
See `../backups/production-compose-rollout-20260907/REPORT.md`.

The verified source artifact remains at:

- VPS: `/opt/schoolheroz-compose-cleanup-preparation-20260907/docker-compose.prod.yml`
- Local: `../backups/production-compose-cleanup-20260907/docker-compose.prod.yml`
- Evidence: `../backups/production-compose-cleanup-20260907/REPORT.md`

The active canonical file is `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`.
It subsequently received the scoped [aggregator installation cleanup](ENERGY_AGGREGATOR.md);
the original staged Compose artifact above is historical, not the current file.
The preceding base file and complete historical configuration are backed up at
`/opt/schoolheroz-compose-cleanup-rollout-20260907`. Old overlays remain archived
in place and are no longer API deployment inputs.

## What the replacement contains

It preserves the production base file's comments and settings. Three explicit
image references select the installed API, Shelly and TTN images. It removes the
obsolete environmental compatibility variable mapping. API loopback binding,
networks, volume names, mounts, restart/logging policies, commands, dependencies
and optional jobs remain equivalent to the verified source configurations.

The Shelly image comes from its own installed collector release; the API's
15-file chain alone did not include that image selection. The checker compares
installed services with their own source chains. Uninstalled/profiled services
retain the production base's resolved contract; they were not started or tested
against providers. The stopped energy aggregator remains stopped.

Existing `/opt/upat-nzc-mqtt-db/.env` supplies the same current effective values;
its observed permission mode is 0600. No secrets are copied into the candidate,
repository or report. The snapshot of API environment values in the old final
overlay is no longer an active deployment input. Always specify the environment
file explicitly rather than relying on the shell's current directory. Shell
exports can override interpolation values; re-run verification in the same
operator environment that will be used for activation.

## Repeat the read-only check

On the VPS, after copying the committed checker to the canonical `ops` directory:

```bash
python3 /opt/upat-nzc-mqtt-db/ops/verify-production-compose.py \
  --project-directory /opt/upat-nzc-mqtt-db \
  --candidate /opt/upat-nzc-mqtt-db/docker-compose.prod.yml
```

The checker reports only differing field paths and booleans. It resolves all
profiles without starting them, checks current container environment values and
image references, compares the full resolved configuration and the API Compose
hash, and checks that container state remains unchanged. It does not deploy.
Unrelated containers without labels are included in the state-stability check
but do not contribute Compose source files. Local regression tests use synthetic
Docker evidence and cover both valid and deliberately divergent configurations.
Do not print or publish raw `docker compose config`: its resolved output contains
secrets.

## Deployment and rollback boundary

Keep project name `upat-nzc-mqtt-db`, project directory `/opt/upat-nzc-mqtt-db`,
and the explicit `.env` path; these preserve bind paths, network and volume
identities. The standard API-only command shape is now:

```bash
docker compose --project-directory /opt/upat-nzc-mqtt-db \
  --env-file /opt/upat-nzc-mqtt-db/.env -p upat-nzc-mqtt-db \
  -f /opt/upat-nzc-mqtt-db/docker-compose.prod.yml \
  up -d --no-deps --no-build --pull never api
```

For future releases, commit the tested source first, build from that exact
revision, and record the resulting image identity before an authorized rollout.
Run this command only after the release preflight. The historical activation used
`--force-recreate` once so the API adopted the one-file configuration labels.
Do not use an untargeted `up`, `down`, or any volume removal. Stopped jobs remain
stopped; this cleanup does not authorize contacting FusionSolar.

`/opt/schoolheroz-compose-cleanup-rollout-20260907/rollback-ready.json` records
the saved base-file path and exact prior API command. Restore the saved base
atomically (after checking no intervening edits) **before** invoking the old
chain. Changing only the command would read the newly installed base instead
of the historical one. The current API image and full configuration archive
were verified before activation. No rollback was needed.

Some other containers still record their historical source files; that is
expected because they were not recreated. Runtime mount lists are compared by
destination, so Docker's nondeterministic list ordering is not mistaken for a
volume change.

## Local checkout boundary

The local and VPS production Compose files now match after the scoped aggregator
installation cleanup. That service has a pinned image, the `jobs` profile and the
original fixed cutover. The former local-only `SHELLY_COUNTER_START` mapping was
reconciled explicitly; development Compose remains unchanged.

This step retains local-only release image references. It does not establish
clean-source builds for all services, image registry distribution, portable host
recovery, or a complete deployment/rollback automation pipeline. Keep unrelated
dirty source changes out of future production builds.
