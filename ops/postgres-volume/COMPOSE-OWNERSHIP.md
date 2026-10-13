# Production PostgreSQL and Compose ownership

PostgreSQL belongs to `upat-postgres-volume.service`, outside Compose.
`iot_postgres` uses `/mnt/HC_Volume_106884142/pgdata`, restart policy `no`
and no Compose ownership labels. Preserve this ownership and data location.

The canonical configuration is
`/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`. It defines no PostgreSQL
service, postgres_data volume or PostgreSQL depends_on. The
`upat-nzc-mqtt-db_default` network is external; Caddy also uses the external
`nzc-energyplus` network. Select reviewed application image/environment changes
through this configuration.

## Application deployment

The VPS `docker-compose.yml` is a relative symlink to `docker-compose.prod.yml`.
The repository's local-development Compose defines PostgreSQL and must not
replace that symlink.

Before a targeted application change, run the installed read-only validator:

```sh
python3 /opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py --check-default
```

It checks the host guard, database, Compose model, image/environment contract
and container stability without emitting credentials or making changes.
A candidate image/configuration still requires its own validation.

Use the exact canonical project, env file and Compose with a named service and
`--no-deps --no-build --pull never`. An untargeted up/down, remove-orphans or
volume operation can affect database ownership and is not an application update.
Keep the source-commit/image mapping; a checkout alone does not reproduce every
pinned production image.

## Storage recovery

`iot_postgres-rootdisk-rollback-20260916` and the former named volume are old
copies. Live writes have continued since their snapshot; restarting them would
discard newer measurements. A data restore needs a reviewed reconciliation
covering all newer data and current consumers.

Use [check-compose.py](check-compose.py) for the maintained ownership validator
and [current recovery requirements](../shelly-compact/RECOVERY.md) for application
storage/time compatibility.

## Startup guards and incident recovery

The [container entrypoint guard](guard-entrypoint.sh) checks the filesystem ID,
PostgreSQL major, cluster identity, owner and permissions, internal WAL and
tablespaces, and a cleanly stopped existing cluster. Missing/empty PGDATA, wrong
identity, `postmaster.pid`, external tablespaces or an unclean cluster blocks
startup. The guard never initializes, repairs, deletes or changes permissions.
After an unexpected crash, diagnose the cluster and review recovery before
restarting; automatic crash recovery is not provided by this strict startup gate.

The [host preflight](host-preflight.py) checks the mount, device, UUID, Volume
serial, filesystem/cluster settings and container mapping against the private
`/etc/upat-nzc/postgres-volume.json`. Its installed path is
`/usr/local/libexec/upat-pg-host-preflight.py`; it does not mount or start anything.
The [systemd unit](upat-postgres-volume.service) requires the real
`mnt-HC_Volume_106884142.mount`, starts after Docker and the mount, and stops when
that mount stops. The provider's `nofail` fstab entry alone is insufficient.
Docker and systemd restart policies remain `no`; an authorized stop uses graceful
SIGINT with no forced-kill timeout. Do not bypass these guards or enable this
unit against the historical root-disk container.

For any newly authorized storage recovery, refresh the live runtime, mount and
cluster identities, writers, schedules and free capacity. Take a fresh consistent
backup, verify its checksum and isolated restore, and keep an off-server copy;
the production Volume is not an independent backup. The September snapshots do
not contain newer measurements. Never run cloned clusters concurrently or
reconnect a historical copy to the application network as a shortcut.

A missing/wrong mount or unclean cluster requires diagnosis. Do not use `initdb`,
automatic directory creation, `postmaster.pid` removal, `docker compose down -v`,
`--remove-orphans`, `COMPOSE_REMOVE_ORPHANS` or volume pruning to bypass it.
The old container may retain Compose labels even though the active database
does not. Removing historical containers or volumes requires a separate review.

A physical cluster copy requires all writers quiesced and PostgreSQL cleanly
stopped. Verify the complete copy with data checksums and metadata comparison;
never copy an active/unclean cluster or overwrite an unexpected nonempty target.
Before releasing writers after a planned restore, verify schema/data/sequence
continuity, correct cluster identity, application DNS and authenticated reads.
Resume only the previously active writers and schedules; verify fresh measurements,
health and logs. The original physical-copy rehearsal did not exercise a real
Hetzner Volume detach or full VPS boot. Those checks remain separate operations.

## Completed migration and evidence

The Volume cutover completed on 2026-09-16. It preserved cluster identity
`7618955918777626661` and database OID `16384` after backup/restore and full stopped
cluster copy verification. Canonical Compose ownership was reconciled later.
The original database image, filesystem UUID, container identities, checkpoint
counts and checksums remain in the
[historical preparation guide](https://github.com/mountzou/upat-nzc-mqtt-db/blob/b6de1fd04635a4145f09c02348cae248e08f17c0/ops/postgres-volume/RUNBOOK.md) and
[completed cutover receipt](https://github.com/mountzou/upat-nzc-mqtt-db/blob/b6de1fd04635a4145f09c02348cae248e08f17c0/ops/postgres-volume/DEPLOYMENT-20260916.md).
Their commands and runtime snapshots are dated evidence; do not replay them as
current operating instructions. The retired renderer and copy helper remain in
Git history.

Historical private evidence locations were
`/Users/mountzou/upat-nzc-mqtt-db/backups/pg-volume-cutover-20260916/`,
`/Volumes/Chris/VPS-Backups/pg-volume-cutover-20260916/` and
`/root/codex-pg-volume-cutover-20260916/`. Preparation evidence was under
`backups/pg-volume-prep-20260916/`. Confirm availability and restore suitability
before relying on any historical backup; retain current off-server backups.
Private runtime inspections and credentials stay outside Git.
