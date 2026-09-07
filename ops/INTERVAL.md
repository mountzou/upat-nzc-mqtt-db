# Interval contract

This document describes the current source contract. The original interval
rollout took place on 6 September 2026; retired bucket parameters and the
public environmental compatibility branch were removed in later rollouts.
Their receipts are historical evidence, not instructions to activate an older
contract.

## Shared notation

| Value | Meaning |
| --- | --- |
| `1m`, `5m`, `15m`, `30m`, `90m` | Fixed elapsed minutes |
| `1h`, `2h`, `24h` | Fixed elapsed hours |
| `day` | A calendar day in Europe/Athens, lasting 23, 24 or 25 elapsed hours |

Only positive integer `m`/`h` values and exact lowercase `day` are accepted.
`60m` normalizes to `1h`; `120m` normalizes to `2h`. `1d`, `0m`, decimals,
negative values and arbitrary PostgreSQL interval expressions return 422.
Each endpoint may further restrict the accepted duration. There is no timezone
query parameter. Stored telemetry remains TIMESTAMPTZ.

## Public environmental history

`GET /indoor_environment/devices/{device_id}/history` uses the canonical
[environmental history contract](ENVIRONMENTAL_HISTORY.md):

- `alignment=clock` (default) accepts `interval=1h` or `interval=day`, with
  complete clock hours or Athens calendar days.
- `alignment=window` accepts fixed minutes/hours and anchors buckets at the
  exact request start; it cannot be combined with `day`.
- Use `window` or a paired aware `start`/`end` range. The default lookback is
  24 elapsed hours, independently of `limit`. Bounds are `[start, end)`.
- `limit` caps requested buckets including empty slots. An oversized request
  returns 422; it does not silently truncate data or extend the lookback.
- `aggregate`, `rolling_1h`, `rolling_24h_hourly`, and all retired `bucket_*`
  parameters return 422, even with blank/false values. The mean is calculated
  server-side; there is no public aggregation selector.

Examples:

```text
/indoor_environment/devices/portable-108/history?window=1h&interval=5m&alignment=window&limit=12
/indoor_environment/devices/portable-108/history?window=24h&interval=1h&alignment=clock
/indoor_environment/devices/portable-108/history?interval=1h&start=2026-10-24T21:00:00Z&end=2026-10-25T22:00:00Z&limit=25
```

The last example covers the 25-hour Athens day of 25 October 2026 and is valid
once that complete range is in the past. Explicit clock bounds must align with
complete buckets; use window alignment for arbitrary instants.

## Energy and retained low-level readers

| Contract | Default interval | Scope |
| --- | --- | --- |
| `/energy/consumption/history` | `1h` | Energy in kWh; see [consumption](CONSUMPTION_HISTORY.md) for date/instant bounds and supported intervals |
| `/energy/production/history`, `/energy/production/forecasts` | `1h` | Energy in kWh; see [production](PV_ENERGY.md) for supported intervals and inclusive Athens dates |
| `/energy/devices/{device_id}/history` | `1h` | Separate device telemetry contract |
| `/upat/device/{device_id}/history`, `/shelly/device/{device_id}/history` | `1m` | Retained low-level readers; fixed duration or `day` |

Listing a low-level route here does not make it publicly accessible. Caddy and
service-token access controls continue to apply.

The retained low-level readers keep `aggregate=avg` (also implied by an explicit
`interval`), a default last-24-hours range, and the existing limit behavior:
explicit start/end ranges take precedence over limit. They accept aware bounds;
legacy offset-free bounds retain their UTC interpretation, including date-only
expansion. This is separate from the strict public environmental contract.
Their maximum interval is 10,080 elapsed days, preserving the former low-level
maximum. Timestamp-range overflow returns 422.

Fixed clock buckets use the UTC origin `2001-01-01T00:00:00Z`; calendar days use
Athens boundaries. Public environmental window alignment instead anchors at
request start. An unaligned low-level range can contain partial edge buckets.
A fixed `24h` interval is therefore distinct from an Athens calendar `day`.

## Consumers and rollout evidence

History callers use `interval` for bucket duration. Retired `bucket_unit`,
`bucket_size` and `bucket_minutes` parameters are rejected before data access;
there is no conversion adapter. Internal numeric minute arithmetic is not an
outgoing compatibility parameter.

Web, iOS and Render request builders preserve explicit instants and Athens
calendar intent. Consumption and PV endpoints have their own energy contracts;
old power-estimate and solar routes are not alternatives to those contracts.

Historical receipts:

- `../backups/interval-rollout-20260906/REPORT.md`: initial compatible rollout.
- `../backups/catalog-interval-retirement-20260907/REPORT.md`: bucket/alias retirement.
- `../backups/environmental-24h-rollout-20260907/REPORT.md`: strict environmental activation.
- `../backups/environmental-final-rollout-20260907/REPORT.md`: compatibility code removal.

Recording this API source requires no new database migration or collection
interruption. Any future deployment must select a tested commit/image pair and
use the matching rollback evidence. No latency or throughput improvement is
claimed solely from parameter cleanup.
