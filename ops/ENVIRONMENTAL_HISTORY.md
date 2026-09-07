# Environmental history

**Production: canonical-only behavior is live on the VPS API as of 7 September 2026.**
Web and iOS Simulator acceptance passed after strict activation; see
`../backups/environmental-24h-rollout-20260907/REPORT.md`.

**Final cleanup is deployed: the disabled compatibility modules and runtime
activation setting were removed in a separate API-only rollout on 7 September
2026.** See `../backups/environmental-final-rollout-20260907/REPORT.md`.
There is one contract for all authenticated
`GET /indoor_environment/devices/{device_id}/history` requests, including bare
requests. The protected low-level UPAT readers used by Render and the internal
readers for latest-minute snapshots and insights retain their existing contracts.
The [retirement note](ENVIRONMENTAL_HISTORY_COMPATIBILITY.md) records the cleanup
boundary and rollback requirements.

## Query contract

| Parameter | Meaning |
| --- | --- |
| `metric` | Optional, repeatable, e.g. `metric=temperature&metric=co2`. Defaults to temperature, relative_humidity, co2, voc, pm25. Repeated names are deduplicated; maximum 32 distinct names. A valid but absent metric produces no reading. |
| `start`, `end` | Paired aware timestamps, inclusive start and exclusive end. Both UTC `Z` and explicit offsets accepted. |
| `window` | Elapsed lookback such as `1h` or `24h`. Cannot accompany start/end. Omission of both forms means 24 elapsed hours, independently of limit. |
| `interval` | Bucket duration; default `1h`. Positive integer minutes/hours, or `day` for an Athens calendar day. `60m` normalizes to `1h`. |
| `alignment` | `window` anchors at the exact start instant; `clock` (default) uses completed clock hours or Athens calendar days. |
| `limit` | Maximum requested buckets including empty slots, 1–1000, default 1000. More slots means 422 before SQL. It does not change lookback or silently truncate data. |

Maximum range is 366 elapsed days. Authentication and school/device authorization remain required. Only `metric` may be repeated. Unknown parameters, duplicate scalar parameters, and any appearance of `aggregate`, `rolling_1h`, `rolling_24h_hourly`, `bucket_unit`, `bucket_size`, or `bucket_minutes` return 422, including blank/false values. There is no precedence rule that silently ignores a supplied control.

### Arbitrary time series / rolling window

At 13:17 Athens time:

```text
/indoor_environment/devices/portable-108/history?window=1h&interval=1h&alignment=window
```

Returns the sample mean for `[12:17,13:17)`. A rolling request preserves seconds/microseconds too: at 13:17:42 it means `[12:17:42,13:17:42)`. For five-minute points in that hour use `interval=5m`. Fixed buckets are anchored at start; the last bucket is clipped to end when the range is not divisible by interval. Explicit future end bounds are permitted in window mode for calendar-based charts; empty future slots are not invented, and the response must not be interpreted as completed-period analytics.

### Completed-hour analytics

```text
/indoor_environment/devices/portable-108/history?window=1h&interval=1h&alignment=clock
/indoor_environment/devices/portable-108/history?window=24h&interval=1h&alignment=clock&metric=temperature&metric=co2
```

At 13:17 these mean `[12:00,13:00)` and `[yesterday 13:00,today 13:00)` respectively. Relative clock windows end at the latest completed boundary. Explicit start/end must already be aligned; unaligned bounds or unfinished clock buckets return 422 rather than silently changing the requested dates. Clock mode supports `1h` and `day`; smaller/arbitrary durations belong to window mode.

### Athens calendar analytics

```text
/indoor_environment/devices/portable-108/history?start=2026-09-01T21:00:00Z&end=2026-09-03T21:00:00Z&interval=day&alignment=clock&limit=2
```

This covers the two complete Athens calendar days 2–3 September. Use paired calendar-midnight bounds for daily queries. `window=24h` always means elapsed time, so it must not be used to represent a 23/25-hour DST calendar day. `day` requires clock alignment; a current unfinished calendar day is not a completed daily statistic. Fixed `24h` remains available in window mode.

## Response and computation

Response fields: `device_id`, resolved `start`, `end`, normalized `interval`, `alignment`, `limit`, `count`, `items`. Items are newest first, with `event_time` as bucket start, explicit exclusive `end`, and measurements containing `value`, `unit`, `sample_count`. All timestamps serialize with the correct Athens offset. Empty histories return 200/count 0/items []; missing buckets or metrics remain absent, never zero-filled. A measured zero remains zero. `sample_count` counts non-null contributing samples for that metric, not expected samples or device-uptime coverage.

PostgreSQL computes `SUM(value_avg * sample_count) / SUM(sample_count)` over complete stored rollups and combines exact raw edges plus samples after the rollup watermark in the same SQL statement. This is a sample-weighted arithmetic mean, not interpolation or a duration-weighted mean. Hour/day clock analytics use hourly rollups; arbitrary windows use five-minute rollups plus raw edges. The sources are disjoint, so late, unprocessed samples are included once. Python only validates and serializes the result; the home history adapter uses the same SQL reader.

For example, one sample at 10 and three at 30 give **25**, not the unweighted mean of the two bucket means (20). Historical values may therefore intentionally differ from the preceding Python re-averaging behavior.

Partially splitting a stored five-minute rollup needs the original samples. Raw retention is currently seven days according to the repository maintenance SQL. If those samples have been removed, the query detects the mismatch against the persisted sample count and returns 422. It neither fabricates a split nor returns an incomplete mean. Aligned retained rollups remain usable. No migration or retention change is part of this batch.

Reads reuse the shared eight-reader semaphore, with a five-second acquisition timeout and ten-second SQL statement timeout. The implementation reduces duplicate aggregation work; no production latency/throughput improvement has been measured yet.

## Current callers and retained boundaries

Verified source callers:

- Web `frontend/src/hooks/useEnvironmentHistory.js`: live → `window=1m&interval=1m&alignment=window&limit=1`; lastHour → `window=1h&interval=1h&alignment=window&limit=1`; trend → `window=24h&interval=1h&alignment=clock&limit=24`.
- Web `frontend/src/lib/environment/environmentFocus.js`: uses no aggregate; hourly chart ranges may use explicit bounds with window alignment. Daily analytics must use complete Athens calendar days, visibly excluding an unfinished today rather than treating it as completed. Keep empty calendar slots in chart presentation.
- iOS `Features/IndoorEnvironment/IndoorEnvironmentService.swift`: uses canonical live and rollingTwentyFourHours requests as above. Hourly pagination must retain explicit bounds and elapsed-hour slot counts (including 169-hour DST weeks), with window alignment when the page includes today. `.hourly(days)` needs an explicit lookback because limit no longer defines it. `.daily(days)` needs Athens calendar start/end, not `days*24` elapsed hours.
- VPS home: the internal 24-hour history adapter now uses the shared reader; its external home response shape is preserved. Its separate live/latest reader is unchanged.
- Render research/AI and the VPS internal general history reader are separate low-level consumers. They keep their existing `aggregate`/limit behavior in this batch; global removal has **not** been performed.

Web and iOS consumers have migrated and passed authenticated live browser/Simulator
checks. This cleanup requires no new consumer contract or data migration.
Successful public history responses retain
`X-Environmental-History-Contract: canonical` and the existing canonical usage
log. These diagnostic markers do not enable an alternate reader.

## Historical transition policy

No retrospective migration, backfill, or raw reconstruction is requested. New requests use the new reader wherever existing inputs support the requested resolution. Historical app requests use complete stored hours/days, while recent rolling requests use available raw detail. Exact historical boundaries that cannot be reconstructed remain explicitly unavailable; there is no silent rounding or fabricated value. This does not change the seven-day raw retention policy or create a persistent deployment-date cutoff.

The public compatibility branch is removed from both the checkout and deployed image. Retired query keys continue
to return 422, exactly as in the deployed strict API. Retained internal readers
are active dependencies and are outside this cleanup.
