# Current Shelly recovery boundaries

Verified baseline, 2026-10-05: API and ingestor use compact-only storage.
`public.shelly_measurements` is absent. `public.shelly_measurements_id_seq`
remains independent (`OWNED BY NONE`). Keep the sequence name/value, compact
series and measurements, readings view, TIMESTAMPTZ instants and one-decimal
Shelly API averages.

## Application rollback

A rollback of application behavior can use a previously verified image only
with its matching compact-compatible configuration. Older images that still
require storage selectors must explicitly select compact reads/writes and the
same rounding policy. Restoring an image alone or selecting legacy/dual storage
would address the retired table and is not a valid recovery method.

The accepted October overlay rollout and its retained image/configuration pairs
are documented in [the rollout receipt](../compact-only-images/ROLLOUT-20261005.md).
Its private recovery materials at
`/opt/schoolheroz-compact-only-20261005-0ac942b9/` are retained. The September
stage scripts and their embedded checkpoints/image identities are historical.

For a future application recovery:

1. Verify the current live files, container/image identities and source of the
   candidate image; confirm its storage, time and rounding contracts.
2. Validate the matching candidate Compose configuration against the PostgreSQL
   ownership guard. Keep secrets, unrelated services and database lifecycle.
3. Rehearse the affected API/history/latest and writer behavior against an
   isolated compact database before any production application change.
4. Target only the approved application services. Account for a possible MQTT
   QoS 0 collection gap during ingestor recreation.
5. Verify internal API health, representative historical/latest responses,
   fresh compact inserts, raw messages/counters and unchanged PostgreSQL state.

This document defines the required compatibility checks; it does not authorize
or execute a production rollback. Schema/data recovery is a separate operation.

## Retired database recovery paths

The old stage 3 reverse-copy recovery required a legacy destination table and
restored dual-write configuration. That destination no longer exists.
`rollback_014_telemetry_timestamptz.sql` required the same legacy table and
converted shared telemetry columns back to UTC-naive timestamps. It has been
removed from active maintenance scripts. Neither is a recovery path for the
current compact database.

Numbered forward migrations, current compact bootstrap/sequence initialization,
retirement receipts and historical checksums remain available. Restoring old
backups or recreating a legacy layout requires a newly reviewed migration that
preserves the entire newer history, IDs, sequence state and current consumers;
the September checkpoints do not cover the data accumulated since then.
