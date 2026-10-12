# Telemetry time and intervals

## Stored instants

UPAT/Shelly measurement times, raw-message times, device creation times and
UPAT rollup starts use TIMESTAMPTZ. Shelly measurements live in
`shelly_compact`; the old `public.shelly_measurements` table is absent.
PostgreSQL stores absolute instants independently of display timezone;
database/API sessions use Europe/Athens.

Migration 014 converted the former UTC-naive telemetry under
`SET LOCAL TIME ZONE 'UTC'`, preserving instants. It is already applied in
production; its completed migration procedure is not an upgrade step.

PV actuals use TIMESTAMPTZ. Plant/device constraints require `local_date`
to equal the Athens date of `observed_at`. TTN requires an offset-bearing
source timestamp and preserves its string/subsecond precision. Shelly creates
aware UTC receipt times; PV validates awareness and source epoch correspondence.

## Wire timestamps

Measurement JSON includes `Z` or an explicit UTC offset. Consumers reject
offset-free measurements, preserve offsets in requests and sort by instants
rather than timestamp strings. Public monitoring history/energy instant bounds
require offsets; invalid bounds return 422. Calendar-date requests retain
their endpoint-specific rules.

Europe/Athens is the fixed application calendar/display timezone. Date pickers,
school hours, daily keys and export boundaries use its IANA rules; repeated
autumn clock hours remain distinct instants. Weather/PV forecast civil timestamps
and simulation calendar/retention metadata keep their declared domain
interpretation; calendar labels are not measurement instants.

## Intervals

| Value | Meaning |
| --- | --- |
| `1m`, `5m`, `15m`, `30m`, `90m` | Fixed elapsed minutes |
| `1h`, `2h`, `24h` | Fixed elapsed hours |
| `day` | An Athens calendar day, lasting 23, 24 or 25 elapsed hours |

Only positive integer `m`/`h` values and exact lowercase `day` are accepted.
`60m` normalizes to `1h`; `120m` to `2h`. `1d`, `0m`, decimals,
negative values and arbitrary PostgreSQL interval expressions return 422.
There is no selectable timezone parameter. Fixed clock buckets use UTC origin
`2001-01-01T00:00:00Z`; calendar buckets use Athens boundaries. Request-window
alignment instead anchors at start. Fixed `24h` is distinct from `day`.

Retired `bucket_unit`, `bucket_size` and `bucket_minutes` parameters return
422 before data access, including empty values or combinations with `interval`.
There is no conversion adapter; internal numeric minute arithmetic is separate.

Each endpoint restricts durations, ranges and limits independently. Its complete
contract is documented in [environmental history](ENVIRONMENTAL_HISTORY.md),
[consumption](CONSUMPTION_HISTORY.md) or [PV production](PV_ENERGY.md).
Device telemetry `/energy/devices/{device_id}/history` is a separate contract
with default interval `1h`.

## Low-level readers

UPAT/Shelly device-history readers default to `1m`, with a last-24-hours
range and `aggregate=avg` also implied by an explicit interval. Explicit
start/end ranges take precedence over limit; limit applies when no explicit
range is supplied. Unaligned ranges may include partial edge buckets.
Their maximum interval is 10,080 elapsed days; timestamp-range overflow
returns 422.

The `parse_telemetry_bound` boundary in `api/schemas.py` accepts aware bounds
and interprets retained offset-free request formats as UTC. Date-only bounds
expand to the UTC day: start at midnight, end at 23:59:59.999999. This request
compatibility is separate from strict measurement parsing and the public
environmental contract. Listing these readers does not grant public access;
[Caddy/service-token controls](SERVICE_ACCESS.md) still apply.

## Validation and recovery

Verify column types, offset-bearing responses, source epoch equality, Athens
PV dates, valid indexes/constraints and fresh ingestion when changing storage.
Use an isolated disposable database for migration or inverse-migration tests.

Application recovery retains TIMESTAMPTZ, compact storage, all current history
and sequence state. The retired inverse of migration 014 requires the removed
legacy table and is not a current recovery procedure. Restoring an older schema
or backup requires a new reviewed plan covering data accumulated since that
snapshot; an application image rollback does not justify reverting the schema.

See [current recovery requirements](shelly-compact/RECOVERY.md),
[PostgreSQL ownership](postgres-volume/COMPOSE-OWNERSHIP.md) and
[consumer responsibilities](TIME_CONSUMERS.md). Historical rollout notes remain
in Git history.
