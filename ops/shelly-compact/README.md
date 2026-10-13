# Shelly compact storage and recovery

## Storage contract

API reads and ingestor writes use compact-only storage. The old storage/write/
rounding selectors are retired. Keep `shelly_compact` series and measurements,
the readings view and the independent `public.shelly_measurements_id_seq`
(`OWNED BY NONE`), preserving its name and current value.

Raw float8 values, IDs, NULLs, units and TIMESTAMPTZ instants are preserved without
rounding. Shelly API averages use `ROUND(AVG(value::numeric),1)::double precision`:
ties round away from zero, so results can differ by 0.1 from former floating-point
rounding. Device metadata, raw MQTT messages, energy counters and hourly energy
tables remain separate contracts; the storage migration did not change UPAT,
PV or simulation job behavior.

Current compact initialization is defined directly in [db/init.sql](../../db/init.sql).
The completed preparation migration 017 remains in the [SQL archive](../../db/archive/README.md).
Do not replay it against the existing production schema.

## Recovery

[RECOVERY.md](RECOVERY.md) owns the current compatibility checks and links to
the API activation/rollback and unchanged Shelly ingestor image records.
Application recovery must preserve compact data, sequence state, timestamps and
reader/rounding behavior, using a verified image with its matching configuration.
Legacy/dual modes and September API images are not a generic fallback.

The legacy `public.shelly_measurements` table is absent. The retired September
reverse-copy recovery required that table and paused the writer to copy/verify
the newer compact tail. It was rehearsed locally and never needed in production;
it cannot be reused after legacy-table retirement. Recreating a legacy layout
would be a new migration needing full-history, sequence and consumer verification.
The removed UTC-naive inverse SQL is also not a current recovery path.

Future operations require fresh backup/restore, consumer, capacity, ownership and
runtime checks. Refresh the writer/consumer inventory before relying on old
equality proofs: the original proof assumed an append-only writer and accounted-for
consumers. Historical updates or additional writers require a new assessment.
Target only approved application services, retaining PostgreSQL's systemd ownership
and the [production Compose contract](../postgres-volume/COMPOSE-OWNERSHIP.md).
Allow for a possible MQTT QoS 0 collection gap during an ingestor interruption;
durable replay of messages during the original pauses was not claimed.

## Completed migration

The staged migration completed on 2026-09-16: prepare/dual-write, bounded backfill,
full-history verification, then compact reads and writes. The final equality proof
combined a read-only repeatable-read snapshot with a drained-writer tail check,
covering IDs allocated before a checkpoint but committed later. Raw values and
existing data contracts were preserved.

On 2026-10-05 the public ID sequence was detached before the legacy table and
its indexes were retired after full history/dependency verification. Approximately
14.54 GB was reclaimed at that checkpoint. The compact-only runtime subsequently
removed the old mode selectors. The stage controllers, migration/recovery helpers,
staged Compose/build files, UTC-naive inverse SQL and seven old VPS stage directories
were retired; their dated checkpoints and image identities are not recovery defaults.

The [historical migration guide](https://github.com/mountzou/upat-nzc-mqtt-db/blob/b6de1fd04635a4145f09c02348cae248e08f17c0/ops/shelly-compact/RUNBOOK.md) preserves
the detailed method, validation boundaries and references. Earlier cutover and
retirement receipts remain in Git history. Future storage changes need a new plan
against the current deployment rather than replaying those completed procedures.
