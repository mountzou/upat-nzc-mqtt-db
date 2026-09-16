# PostgreSQL Volume migration preparation — 2026-09-16

Status: the separately authorized production cutover completed on 2026-09-16.
See `DEPLOYMENT-20260916.md` for current ownership, retained-copy restrictions and
verification receipts. The sections below preserve the reviewed preparation and
procedure; do not rerun them against the completed migration. Future changes and
cleanup require a new scoped user instruction.

## Observed installation

- VPS: `65.21.106.41`, project `/opt/upat-nzc-mqtt-db`.
- PostgreSQL: `iot_postgres`, 15.17 amd64, database `iot_db` (OID 16384).
- Image: `sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399`.
- Cluster identity: `7618955918777626661`.
- Current source: named volume `upat-nzc-mqtt-db_postgres_data`, host path
  `/var/lib/docker/volumes/upat-nzc-mqtt-db_postgres_data/_data`.
- Approximately 18.6 GB allocated, UID:GID 999:999, mode 0700; WAL is inside
  PGDATA and there are no external tablespaces. Recheck these before copying.
- Destination: `/mnt/HC_Volume_106884142/pgdata` on Volume `106884142`, ext4,
  UUID `261f006b-608a-41c3-9ed9-0023d70e1922`, filesystem ID `4800c6b8e15884b7`.
- Device: `/dev/disk/by-id/scsi-0HC_Volume_106884142` (currently `/dev/sdb`).
- Destination is still empty apart from `lost+found`; no `pgdata` has been
  created in production by this preparation.

All IDs, capacity figures, service states, and configuration checks are time
specific. The private `production-spec.json` and `production-create.private.json`
in the rehearsal evidence directory are review artifacts, not a license to
apply stale configuration later. No secret-bearing payload belongs in Git.

## Prepared components

`render-runtime.py` renders a Docker Engine create body offline from the approved
full container inspect. It preserves the running installation's command,
healthcheck, existing environment, remaining mounts, logging, resource settings,
ports, and network aliases. It pins the exact image, replaces the PGDATA mount,
adds the read-only entrypoint guard and three identity settings, and gives the
container a fresh hostname. Compose ownership labels are removed deliberately:
this database will be owned by the explicit systemd unit. Container name and
network aliases remain `iot_postgres` / `postgres`.

The existing Compose file does not reproduce the actual runtime. Do not run
broad `docker compose up/down`, automatic migrations, image upgrades, or recreate
other services as part of this storage change. Before future Compose deployments,
reconcile database ownership in version control and restrict service selection;
the preserved `iot_postgres` name should cause a conflicting create to fail,
but that is not a replacement for a reviewed deployment procedure.

`guard-entrypoint.sh` runs before the official image entrypoint. It requires the
correct filesystem ID, PostgreSQL major, cluster identity, owner, permissions,
internal WAL/tablespaces, and a cleanly stopped existing cluster. Empty PGDATA,
wrong identity, a `postmaster.pid`, or an unclean cluster fails closed. It does
not initialize, delete, repair, or change permissions. In particular, after an
unexpected crash it intentionally requires manual diagnosis/recovery review;
this plan trades automatic restart availability for a strict startup gate.

`host-preflight.py` adds host-side mount/device/UUID/serial and container-mapping
checks. `upat-postgres-volume.service` requires the exact mount, starts after
Docker and the mount, stops when the mount unit stops, and uses a graceful
SIGINT stop without a forced-kill timeout. Docker restart policy remains `no`.
The provider's existing `nofail` fstab entry alone is not sufficient protection.
Do not enable this unit against the current root-disk container.

`copy-stopped-cluster.sh` requires a stopped source, matching cluster ID, and an
empty verified destination. It preserves metadata using `rsync -aHAX
--numeric-ids`, syncs the destination, then reads both copies in full with
`rsync --checksum --dry-run --delete --itemize-changes`. `--delete` appears only
in a dry run to detect extra files; the actual copy never deletes anything.
The source must be mounted read-only in the helper. The caller must prevent all
other processes from starting that source until the verification finishes.

## Separately authorized production procedure

1. **Refresh facts and backup.** Confirm free root space, Volume identity,
   original runtime/container ID, PG start time, image availability, tablespaces,
   file ownership and actual writers. Record all active and stopped services,
   cron/systemd timers, maintenance scripts, network aliases and scheduled jobs.
   The earlier maintenance pause has already ended; do not reuse its old state.
   Take a fresh consistent complete backup and preserve it on Mac and Chris,
   with checksums and restore evidence. The rehearsal used an earlier verified
   snapshot; it does not contain subsequent production measurements.
2. **Stage reviewed prerequisites.** Install versioned scripts and the private
   root-owned spec, inspect permissions/hashes and verify the unit on the real
   host without starting/enabling it. Use an audited local helper image matching
   PostgreSQL 15.17 and providing rsync; transfer/load a pinned tested image if
   needed and account for its disk use. Do not install packages or build images
   on the nearly full root filesystem implicitly. The tested helper runs the
   copy script; a host-only rsync alternative requires an explicit equivalent
   `pg_controldata` check with the exact PostgreSQL image first.
3. **Quiesce every writer.** Enter the agreed maintenance window; stop only the
   previously inventoried active ingestors, write-capable API/jobs and schedules.
   Drain transactions, verify no writers/retry loops, record final DB catalog,
   counts/sequences/freshness and backup checkpoint. Leave previously stopped
   `schoolheroz-compact-candidate` and `energy_aggregator` stopped. An ingestor
   pause can leave the already accepted measurement gap.
4. **Stop only PostgreSQL cleanly.** Use SIGINT with an unlimited graceful-stop
   timeout; observe clean shutdown logs, exit 0, no OOM and `pg_controldata`
   state `shut down`. Never force kill to meet a deadline. If this fails, stop
   the migration and investigate; do not copy an active or unclean cluster.
5. **Copy the whole stopped cluster.** Independently recheck mounted device,
   UUID, serial, filesystem ID and free capacity before creating `pgdata`.
   Mount the exact source read-only and destination read-write into the reviewed
   isolated helper (`--network none`); run the copy script with freshly verified
   production cluster/FS identities. Verify the complete checksum/metadata pass,
   `pg_control`, PG_VERSION, owner and size. The old volume remains intact.
   A partial/nonempty destination is a stop condition, never an implicit wipe.
6. **Hand over container identity.** Keep writers stopped. Retain the old stopped
   container under a unique rollback name, e.g.
   `iot_postgres-rootdisk-rollback-20260916`, and explicitly disconnect its
   application network before starting the candidate, to avoid alias ambiguity.
   Render/review the create payload against the freshly captured original
   runtime; the only intended changes are those described above. Create the new
   `iot_postgres` with the pinned image, structured bind mounts and preserved
   `postgres`/`iot_postgres` aliases. Do not run two cloned clusters concurrently.
7. **Validate before application writes.** Run the host preflight and guarded
   systemd startup. Require original cluster identity, database OID, healthy
   startup, expected schema/objects/sequences/counts and unchanged freshness
   checkpoint, correct mount, no initdb, no recovery errors. Confirm DNS from
   the actual application network and read-only application queries. Record
   findings before resuming writers; local `network none` tests cannot prove
   production DNS/client behavior. Enable the reviewed unit only when its live
   wiring is verified; do not reboot the VPS merely to test it without separate
   authorization.
8. **Resume and observe.** Resume exactly the previously active services and
   schedules in their recorded order. Verify newly persisted measurements,
   application health, logs, capacity and no restarts. Restore scheduling and
   maintenance state explicitly. Keep the original source for the agreed
   rollback window, then request the separate cleanup decision with evidence.

There is no deletion of the original Docker volume, no schema conversion and no
PostgreSQL version upgrade in this procedure. Moving to the Volume does not
immediately reclaim the old root-disk space while the original copy is retained.
Future new data grows on the Volume; root space is reclaimed only after a later,
explicitly verified cleanup. The Volume is not an independent backup; continue
off-server backups (including Mac/Chris as agreed).

## Rollback boundary

Before enabling application writers, cleanly stop the candidate, disconnect its
application network, preserve it under a diagnostic name, restore the original
container name and recorded network aliases, start the original clean cluster,
verify it, then resume the recorded writers. Do not change or delete either copy.
Disable any newly enabled candidate-only unit before this rollback so it cannot
start the old container accidentally; restore the original service ownership.

After production writes reach the new cluster, the old copy is stale. **Do not
simply restart the old copy**: that would discard newer measurements. A rollback
then requires a new consistent copy/backup of the current cluster or a separately
verified recovery strategy. The local write test intentionally demonstrates
isolation of the copies; it is not a post-write production rollback strategy.

A missing/unmounted Volume, wrong identity, or unclean cluster is an incident to
investigate. Never fix it with `initdb`, automatic directory creation, removal of
`postmaster.pid`, `docker compose down -v`, or a volume deletion.

## Local rehearsal and evidence

Evidence lives under `backups/pg-volume-prep-20260916/` (private and gitignored).
The original verified backup is retained unchanged in
`backups/cx33-final-checkpoint-20260916/` and on Chris.

The rehearsal uses the exact PostgreSQL image on `linux/amd64` in Docker Desktop,
with no published ports and `--network none`. It restores the full snapshot,
checks 29 tables / 72,031,999 rows and 18 sequences, and performs a physical copy
between separate local volumes. Deterministic binary COPY SHA-256 values over
all columns/rows ordered by primary key compare source, candidate and rollback.
The rehearsal's cluster identity is newly generated by the restore and is
expected to differ from the production cluster identity.

Negative startup cases cover wrong filesystem, wrong cluster, wrong major,
missing bind source and empty PGDATA. Positive tests include successful guarded
startup, writes surviving a clean restart, source file hashes unchanged during
candidate operation, and switching back to the original local copy. Both test
clusters are left stopped with volumes retained. The source backup is unchanged.

Check `REHEARSAL-VERIFIED.json`, `RUNTIME-RENDER-VERIFIED.json`,
`PRODUCTION-UNCHANGED.json` and `REPORT.md` for actual outcomes. This document
alone is not a PASS receipt. Docker Desktop cannot exercise a real Hetzner
Volume disconnect or Ubuntu systemd boot. The unit's local syntax verification
uses explicitly identified Docker/mount stubs; it is not a boot integration test.
All local timings are diagnostic, not a production downtime promise.

## References

- PostgreSQL 15 filesystem-level backup and stopped-cluster/rsync requirements:
  https://www.postgresql.org/docs/15/backup-file.html
- Docker bind mount creation semantics:
  https://docs.docker.com/engine/storage/bind-mounts/
