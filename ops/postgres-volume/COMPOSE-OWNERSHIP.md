# Production Compose after the PostgreSQL Volume cutover

PostgreSQL is exclusively owned by `upat-postgres-volume.service`. The production
Compose model contains application services only, no `postgres` service, no
`postgres_data` volume and no application `depends_on: postgres` entries.
The shared `upat-nzc-mqtt-db_default` network is external: Compose reuses it and
must not create/remove it. Database hostname, credentials, schema, images and
application configuration remain unchanged.

## Versioned installation mapping

- Repository `docker-compose.prod.yml` -> `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`.
- VPS `/opt/upat-nzc-mqtt-db/docker-compose.yml` -> relative symlink `docker-compose.prod.yml`.
- `ops/postgres-volume/check-compose.py` and this document retain their repository paths on the VPS.
- The repository's development `docker-compose.yml` remains a local development
  file. **Do not copy it over the VPS symlink or deploy the whole checkout.**
- The live source baseline was recorded at `d837dcb`. Its API image and simulation
  polling settings were already deployed; this change does not release application code.
- `install-compose.py` validates all staged hashes and both prior Compose hashes,
  performs a read-only preflight, saves prior files, installs only the listed
  files, then rechecks state. It never runs up/down/start/stop/restart or a build.
  Verification failure restores configuration files without applying services.

## Before any future targeted deployment

```sh
python3 /opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py --check-default
```

This validates the current systemd/mount guard, running DB identity, internal
network/alias, no published PostgreSQL port, Compose ownership, service images,
resolved environment and unchanged container states. It prints no credentials.
For an application release that deliberately changes images/environment, first
check the currently installed configuration, then separately validate the new
release contract. This check rejects intentional differences from the running
containers until that separately approved release is applied.

Use an explicit service with the existing canonical project and environment:

```sh
# Shape only; run after authorization for that application release.
docker compose --project-directory /opt/upat-nzc-mqtt-db \
  --env-file /opt/upat-nzc-mqtt-db/.env -p upat-nzc-mqtt-db \
  -f /opt/upat-nzc-mqtt-db/docker-compose.prod.yml \
  up -d --no-deps --no-build --pull never api
```

Compose no longer provides a PostgreSQL startup/health dependency. The preflight
must pass before an application deployment. It does not start the DB automatically.
The existing jobs retain their profiles and `--no-deps` entrypoints, with no cron,
timer, provider or writer execution needed for this installation. No service
restart is needed to install these configuration files. Existing container labels
and configuration hashes describe their earlier creation and do not change until
a separately authorized recreation.

## Retained historical container and limits

`iot_postgres-rootdisk-rollback-20260916` remains stopped, disconnected, and retains
old Compose labels. It may be reported as an orphan: **do not use `--remove-orphans`,
`COMPOSE_REMOVE_ORPHANS`, broad `down`, or volume pruning**. The active DB has no
Compose ownership labels. Removing the old container and its named volume is a
separate approved operation. No old data is deleted by this config installation.

Archived historical overlays and database maintenance scripts are not current
production deployment inputs. Do not run their old commands without a new review.
The former `ops/verify-production-compose.py` checks the pre-cutover contract;
use the new `check-compose.py` for the active systemd-owned database architecture.

Backups of previous configuration are historical evidence, not a safe DB rollback.
Do not restore pre-cutover Compose ownership without a fresh migration plan.
The existing strict DB startup guard, restart policy, and systemd unit are unchanged.
This step does not test a VPS reboot or Volume detach.

## Validation

- Real Compose resolution compares the candidate with the captured live baseline;
  only database service/dependencies/volume removal and external-network ownership
  may differ. Profiles and all remaining settings must match exactly.
- Synthetic regression cases reject reintroduced DB mounts, dependencies, volume,
  ownership and unrelated application image/environment changes.
- A separate Docker Desktop test uses inert shell containers and an internal test
  network. Application up/down preserves an external DB sentinel and stopped old
  orphan; targeting `postgres` fails. It runs no PostgreSQL server or provider call.
- On the VPS, candidate/default model equivalence and runtime state are checked
  read-only before/after installation; no Compose lifecycle command is executed.

Docker reference: [external network lifecycle](https://docs.docker.com/reference/compose-file/networks/#external).
