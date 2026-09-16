# Completed production Volume cutover — 2026-09-16

The authorized cutover completed successfully. PostgreSQL is serving production
from `/mnt/HC_Volume_106884142/pgdata` on Hetzner Volume `106884142`.
The VPS was not rebooted. No database/table/index or original volume was deleted.

## Recorded identities

- Operational code deployed from commit `5f90925` on
  `codex/pg-volume-prep-20260916`; initial guard/copy implementation `9aed560`.
- PostgreSQL image unchanged:
  `sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399`.
- Tested copy-helper image:
  `sha256:5436b7ee304b830f48edb48b9bd5b4885610a28f689996497cc42afb322f1899`.
- New `iot_postgres` container:
  `3e8abad67a5a4bbfdb2dd0773287d808e83b49a294634cff9d4d57af8efc5e1d`.
- Original cluster identifier retained: `7618955918777626661`.
- Original `iot_db` OID retained: `16384`.
- Volume UUID: `261f006b-608a-41c3-9ed9-0023d70e1922`.
- Original stopped container:
  `iot_postgres-rootdisk-rollback-20260916`, ID
  `c7b333bccb47d319b87f620281ccd4a467dda858173fffe87a7577695ca0525d`.
- Original retained named volume: `upat-nzc-mqtt-db_postgres_data`.

## Data verification and service restoration

A fresh full logical backup was restored locally: 29 tables, 72,064,030 rows,
18 sequences, matching schema/indexes/constraints, and successful local restart.
The backup and approved private configuration files were copied to Mac and Chris,
with every copied file read back and verified by SHA-256 before maintenance.

After all writers were paused, the final checkpoint contained **72,078,184 rows**
in 29 tables. The entire cleanly stopped cluster was copied with the source
mounted read-only. Full rsync checksum/metadata comparison passed in 237.9 seconds
including the copy. Before writer release, counts, all 18 sequences, schema,
cluster/database IDs and final Shelly/UPAT measurement checkpoints matched exactly.

Three real authenticated read-only connections using the existing API and ingestor
credentials reached the correct database through the application network. Original
API/ingestor containers and exact active cron entries were restored, along with all
three previously enabled timers. New Shelly and UPAT/TTN measurements persisted;
API `/health` reported `status=ok, database=connected`. Caddy, Mosquitto and the two
previously stopped containers retained their original states/start times.

Times below are UTC (add three hours for Greece):

- API paused: 14:30:02; ingestors stopped: 14:31:36.
- Original PostgreSQL clean stop: 14:32:40.
- New PostgreSQL ready: 14:37:17 (about 4 minutes 37 seconds database downtime).
- Application containers resumed: 14:40:40–14:40:41 (about 9 minutes ingestor pause).
- First live verification: 14:41:14, including new readings from both streams.

The measurement ingestion pause is distinct from loss of stored data. All
previously persisted data passed before/after verification; incoming messages
while ingestors were stopped may have the accepted collection gap.

## Current operating ownership

`upat-postgres-volume.service` is active and enabled. Its real mount dependency
and installed guard files were verified on the VPS. Docker restart policy remains
`no`; the wrapper rejects an absent/wrong filesystem, empty directory, wrong
cluster/version, or an unclean cluster. The service requires and binds to
`mnt-HC_Volume_106884142.mount`.

For future authorized database operations, use this systemd unit and its documented
host preflight. Do not bypass the guard, reinitialize PGDATA, or remove a stale PID
without diagnosing the cluster. A crash needs manual recovery review under this
strict startup policy. No physical Volume detach or full VPS boot test was performed.

**Do not run broad Compose operations involving PostgreSQL.** The production
Compose configuration predates this storage ownership change. The retained old
container still has its original Compose labels and could be rediscovered by
Compose. Existing audited scheduled Compose jobs use `--no-deps` and therefore do
not start the database. Before any future broad Compose deployment, reconcile this
ownership in a separately reviewed change. Do not start the retained old container
or reconnect it to the application network.

The old volume is now a historical pre-cutover copy: production writes have
resumed on the new Volume. Simple rollback to the old copy would lose those newer
writes. The dated controller explicitly rejects that rollback after writer release.
Any later recovery must preserve the current cluster's newer data first.

The retained old copy still occupies the root disk. Root use remained around 77%;
new PostgreSQL growth now goes to the Volume, which had about 57 GiB available at
first verification. Reclaiming the old root-disk space remains a separate approved
cleanup step after stable operation and current backup verification.

## Evidence and operational notes

Private Mac evidence and fresh backup:
`/Users/mountzou/upat-nzc-mqtt-db/backups/pg-volume-cutover-20260916/`.
External backup:
`/Volumes/Chris/VPS-Backups/pg-volume-cutover-20260916/`.
VPS receipts:
`/root/codex-pg-volume-cutover-20260916/`.

Key receipts: `RESTORE-VERIFIED.json`, `COPY-VERIFIED.json`,
`completion/checkpoint-before.json`, `completion/checkpoint-after.json`,
`completion/COPY-CLUSTER-VERIFIED.json`, `completion/NETWORK-SQL-VERIFIED.json`,
`completion/RESUMED.json`, `LIVE-VERIFIED.json`, and the staged commit/hash manifest.
Secret-bearing inspections/create payloads remain private and outside Git.

Host-specific adjustments recorded in commits:

- `systemd-analyze verify` must be passed the actual generated mount unit file
  explicitly on this host; invoking all generators was not a valid substitute.
  This was resolved before downtime.
- The Python ingestors use the previously established SIGINT stop; SIGTERM did
  not terminate their PID 1. No SIGKILL was used. PostgreSQL itself shut down cleanly.

Do not rerun the dated controller against this completed migration. Refresh live
facts and prepare a new operation for any subsequent storage/schema change.
