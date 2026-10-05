# Shelly compact migration history

Current-state update, 2026-10-05: the legacy table has been retired and the
shared public ID sequence is free-standing. See [the retirement receipt](LEGACY-RETIRED-20261005.md).
The sequence ownership and legacy-retention descriptions below record the
September migration state.

The staged migration completed on 2026-09-16. Its controllers, migration/recovery
helpers and staged Compose files have been retired; their source remains in Git
history. This document summarizes the method and its constraints. Future operations
require a new design against the current [production Compose](../../docker-compose.prod.yml)
and [PostgreSQL ownership contract](../postgres-volume/COMPOSE-OWNERSHIP.md).

## Preserved contracts

The migration changed Shelly measurement storage and its API history/latest readers.
Device metadata, raw MQTT messages, energy counters, hourly energy tables, UPAT,
PV and simulation jobs retained their existing behavior. Raw float8 values, IDs,
NULLs, units and timestamptz values were preserved without rounding. Only Shelly API
averages use `ROUND(AVG(value::numeric),1)::double precision`: ties round away from
zero, so a previous floating-point result can differ by 0.1.

PostgreSQL remained under `upat-postgres-volume.service`, with data on
`/mnt/HC_Volume_106884142/pgdata`. Application changes were targeted recreations;
the database and unrelated services stayed running. The rollout did not rename or
delete the legacy table, rebuild its indexes or run VACUUM FULL. Brief ingestor
pauses occurred; durable replay of QoS0 MQTT messages during those pauses was not
claimed.

## Completed migration

1. **Prepare and dual write.** New `shelly_compact` objects were created after a
   verified backup/restore and consumer audit. Measurement writers were paused and
   drained; a source SHARE lock protected capture of the forward checkpoint.
   This prevented skipping transactions whose IDs had been allocated before the
   checkpoint but committed later. The ingestor then wrote both layouts, raw
   messages and counters atomically in the same transaction. API reads stayed legacy.
2. **Backfill and index.** Historical rows were copied in bounded, resumable batches.
   Each batch committed its rows and cursor together; existing IDs were accepted
   only when all original fields matched. Available space, retained WAL and service
   load were monitored. The covering index was built CONCURRENTLY after backfill,
   followed by ANALYZE on the new objects and index/constraint validity checks.
3. **Verify and switch reads.** All six original fields were streamed in ID order
   from both layouts in one READ ONLY REPEATABLE READ snapshot. SHA-256, byte counts,
   row counts and ID bounds were compared across the full dataset. Atomic dual
   inserts were visible on both sides or neither. API comparisons used the same
   `decimal_1` policy before switching reads to compact; the ingestor remained dual.
   The existing unbounded latest query remained potentially expensive.
4. **Switch writes.** A fresh backup, isolated restore and external-disk checksum
   readback preceded the final cutover. After full verification with the dual writer
   active, the ingestor was stopped/drained and short SHARE locks protected tail
   verification from the original stage-1 barrier through the final ID. This covered
   late commits beyond the snapshot and certified a reverse checkpoint without
   recopying the full history. Only the writer mode changed from dual to compact;
   live new measurements, API results, raw ingestion and counters were checked.

The final equality proof combined the verified full snapshot with the drained-writer
tail check. It depended on the audited append-only writer and accounted-for consumers.
Historical updates or additional writers require a new assessment.

## Current recovery boundary — 2026-10-05

The legacy table has been removed. The active ID allocator remains
`public.shelly_measurements_id_seq`, independent with `OWNED BY NONE`. Its name
is an active compatibility contract. About 14.54 GB was reclaimed at retirement.

Application recovery must retain `shelly_compact` data, the independent sequence,
TIMESTAMPTZ instants and the current reader/rounding contract. Use a previously
verified compact-compatible image together with its matching configuration;
legacy/dual modes and September API images are not a generic fallback.
See [RECOVERY.md](RECOVERY.md) for the current checks and scope.

The September reverse-copy implementation required a retained legacy table and
paused the writer while copying/verifying the newer compact tail. It was tested
locally and was never needed on production. It has been retired and cannot be
reused after the legacy table was dropped. Recreating a legacy layout would be
a new data migration requiring its own full-history, sequence and consumer proof.
Raw MQTT messages and energy counters remain separate operational data.

## Evidence

The records below retain commits, image identities, backup/restore results,
checkpoints, full-data counts/checksums and private evidence locations:

- [Local validation](VALIDATION-20260916.md): isolated full-size rehearsal and
  integration tests, with their coverage and limitations.
- [Stage 1](DEPLOYMENT-STAGE1-20260916.md): dual writes, backfill and full equality.
- [Stage 2](DEPLOYMENT-STAGE2-20260916.md): compact API reads and response comparisons.
- [Stage 3](PRODUCTION-STAGE3-20260916.md): compact-only writes, certified checkpoint
  and post-cutover observations, including the limits of runtime verification.

These are dated migration receipts; future operations need fresh backup/restore,
consumer, capacity, ownership and runtime checks. PostgreSQL references:
[snapshot isolation](https://www.postgresql.org/docs/15/transaction-iso.html) and
[sequence behavior](https://www.postgresql.org/docs/15/functions-sequence.html).
