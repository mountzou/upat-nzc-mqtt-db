# Environmental history

Authenticated `GET /indoor_environment/devices/{device_id}/history` uses one
strict public contract, including bare requests. School/device authorization
applies to every read. Successful responses include
`X-Environmental-History-Contract: canonical` and the canonical usage log.

## Query contract

| Parameter | Meaning |
| --- | --- |
| `metric` | Optional and repeatable; defaults to temperature, relative_humidity, co2, voc, pm25. Names are deduplicated, with at most 32 distinct names. A valid but absent metric produces no reading. |
| `start`, `end` | Paired timestamps with `Z` or an explicit UTC offset; inclusive start, exclusive end. |
| `window` | Elapsed lookback such as `1h` or `24h`; cannot accompany start/end. Omission of both forms means 24 elapsed hours, independently of limit. |
| `interval` | Default `1h`; positive integer minutes/hours, or `day` for an Athens calendar day. `60m` normalizes to `1h`. |
| `alignment` | `clock` (default) selects complete clock hours/calendar days; `window` anchors buckets at the exact start instant. |
| `limit` | Requested bucket cap, including empty slots: 1–1000, default 1000. Oversized ranges return 422 before SQL; limit never changes or truncates the lookback. |

Maximum range: 366 elapsed days. Only `metric` may be repeated. Unknown
parameters, duplicate scalar parameters and the retired `aggregate`,
`rolling_1h`, `rolling_24h_hourly`, `bucket_unit`, `bucket_size` and
`bucket_minutes` return 422, including blank/false values.

## Alignment and examples

- **Window:** fixed minutes/hours, preserving start seconds and microseconds.
  The last bucket is clipped to end. Explicit future end bounds are allowed,
  but empty future slots are absent and the range is not completed-period analytics.
- **Clock:** only `1h` or `day`. Relative windows end at the latest completed
  boundary. Explicit bounds must already align; unaligned or unfinished buckets
  return 422.
- **Calendar day:** `day` requires clock alignment and Athens midnight bounds.
  A day lasts 23, 24 or 25 elapsed hours across DST. Use explicit calendar
  bounds; `window=24h` and fixed `interval=24h` always mean elapsed time.

At 13:17 Athens, `window=1h&interval=1h` covers [12:17,13:17) with window
alignment or [12:00,13:00) with clock alignment. At 13:17:42, the window starts
at 12:17:42. A clock-aligned 24-hour window ends at today's 13:00.

```text
# Live, rolling hour, completed-hour trend, two complete Athens calendar days
/indoor_environment/devices/portable-108/history?window=1m&interval=1m&alignment=window&limit=1
/indoor_environment/devices/portable-108/history?window=1h&interval=1h&alignment=window&limit=1
/indoor_environment/devices/portable-108/history?window=24h&interval=1h&alignment=clock&limit=24
/indoor_environment/devices/portable-108/history?start=2026-09-01T21:00:00Z&end=2026-09-03T21:00:00Z&interval=day&alignment=clock&limit=2
```

Clients supply explicit lookbacks/ranges for pagination; limit only caps buckets.
Daily analytics exclude an unfinished today. Charts may display empty calendar
slots without treating them as observations.

## Response and calculation

Fields: `device_id`, resolved `start`/`end`, normalized `interval`,
`alignment`, `limit`, `count`, `items`. Items are newest first, with
`event_time` as bucket start, exclusive `end`, and per-metric `value`,
`unit`, `sample_count`. Timestamps include the correct Athens offset.

Empty histories return 200 with count 0 and items []. Missing buckets/metrics
stay absent; measured zero stays zero. Sample count is the number of non-null
contributing samples, not expected coverage or device uptime.

PostgreSQL calculates `SUM(value_avg * sample_count) / SUM(sample_count)`,
combining complete rollups, exact raw edges and samples after the rollup watermark
in one query. Disjoint sources include late samples once. Clock analytics use
hourly rollups; window queries use five-minute rollups plus raw edges. This is a
sample-weighted arithmetic mean, without interpolation or duration weighting:
one sample at 10 and three at 30 produce 25. Python validates and serializes;
Home's 24-hour history uses the same reader.

Partial rollup splits require the original measurements. The repository's
high-resolution measurement retention defaults to seven days. If retained sample
counts cannot be reconciled with those measurements, the reader returns 422
rather than an incomplete mean. Aligned retained rollups remain usable; there is
no deployment-date cutoff, historical backfill or fabricated raw reconstruction.

Reads share eight monitoring slots, with a five-second acquisition timeout and
ten-second SQL statement timeout. No production performance improvement is
claimed from this contract alone.

## Internal readers and recovery

Home's separate live/latest reader and environmental insights retain their
internal contracts. Protected low-level Render research/AI readers also retain
their existing aggregate/limit behavior; those arguments are active dependencies.

The public compatibility parser and former Python history reader were retired
on 7 September 2026. `ENVIRONMENT_HISTORY_LEGACY_COMPAT` has no effect and cannot
restore the removed implementation. Historical rollout notes remain in Git history.

Application recovery requires a verified API image and matching configuration
compatible with current consumers, compact Shelly storage and TIMESTAMPTZ.
September image/configuration snapshots are historical and must be reviewed
against the current contracts before reuse. See the
[production ownership guide](postgres-volume/COMPOSE-OWNERSHIP.md) and
[current recovery requirements](shelly-compact/RECOVERY.md).
