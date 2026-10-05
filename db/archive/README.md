# Historical SQL archives

These files preserve completed migration source outside the active migrations
and bootstrap paths. Their original SQL is unchanged. They are historical
records, not commands to execute against the current database.

## Shelly compact preparation — migration 017

Archived on 2026-10-05 from `db/migrations/017_shelly_compact_prepare.sql` to
[migrations/017_shelly_compact_prepare.sql](migrations/017_shelly_compact_prepare.sql).
The file is retained byte-for-byte from commit `9239c92`; its number is preserved
and will not be reused or renumbered.

It prepared compact storage for the September dual-write/backfill migration,
including `shelly_compact.copy_progress` for forward/reverse copy checkpoints.
Those migration controllers have been retired. Its sequence-ownership comments
and mixed-mode assumptions describe that historical phase.

Current `db/init.sql` directly initializes the independent public ID sequence,
compact series/measurements, covering index, readings view and resolve_series
function. It never calls migration 017. No current repository script/test or
VPS service/cron reference to the archived migration was found at retirement.

The archived SQL is not idempotent and must not be replayed against today's
compact database. Archiving this file does not drop or alter any existing table,
function, view, sequence or data. Current recovery is documented in
[RECOVERY.md](../../ops/shelly-compact/RECOVERY.md).
