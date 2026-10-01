# Production PostgreSQL and Compose ownership

Verified against the VPS on 2026-09-29. PostgreSQL is exclusively owned by
`upat-postgres-volume.service`, outside Compose. The live container `iot_postgres`
uses `/mnt/HC_Volume_106884142/pgdata`; its Docker restart policy is `no` and it
has no Compose ownership labels. The host guard and unit in this directory match
the installed files by SHA-256. The active database and its data must never be
recreated through Compose.

The canonical `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml` matches the
repository `docker-compose.prod.yml` as of this verification. It defines no
`postgres` service, `postgres_data` volume or PostgreSQL `depends_on`. The
`upat-nzc-mqtt-db_default` network is external. Caddy also uses the external
`nzc-energyplus` network. Current production image references and Shelly read/write
modes are recorded in this Compose file. These are current deployment facts, not
instructions to recreate the running containers.

## Deployment boundary

- The VPS `/opt/upat-nzc-mqtt-db/docker-compose.yml` is a relative symlink to
  `docker-compose.prod.yml`. The repository `docker-compose.yml` is for local
  development and still defines a PostgreSQL service. Never install that file
  over the VPS symlink.
- Before a separately authorized, targeted application release, run the installed
  read-only validator:
  `python3 /opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py --check-default`.
  It checks the host guard, active database, Compose model, images, environment
  and container stability. It emits no credentials. A future intentional image
  or environment change requires a separately reviewed release contract.
- The validator is read-only. It does not make a new image from the repository,
  apply a migration, restart a service, or exercise a reboot or Volume detach.
- If an application release is later authorized, use the exact canonical project,
  `.env`, Compose file and a named service with `--no-deps --no-build --pull never`.
  Never use an untargeted `up`, `down`, `--remove-orphans` or volume pruning.
- `iot_postgres-rootdisk-rollback-20260916` is a stopped, disconnected historical
  container with old Compose labels. The old named volume is stale after live
  writes resumed. Neither is a direct rollback target.

## Historical migration material

`RUNBOOK.md`, `DEPLOYMENT-20260916.md` and the Shelly migration documentation
record the completed September migration. The staged Shelly controllers,
migration/recovery helpers and Compose snapshots have been retired from the
repository; their source remains in Git history. The recorded commands and
baseline hashes are historical evidence. Use
[check-compose.py](check-compose.py) for the active systemd-owned database
architecture.

The current production Compose also pins a simulation-recorder image built for
its September 28 release. The matching recorder source is being reconciled in
separate draft PR #3. Until that is merged, this checkout alone does not
reproduce that image build; preserve the pinned image on the VPS.
