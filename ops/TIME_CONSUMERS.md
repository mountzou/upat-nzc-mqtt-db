# Telemetry timestamp consumers

Use the shared [timestamp and interval contract](TIME_PERSISTENCE.md) for stored
instants, wire offsets, Athens calendars and duration notation.

## Consumer responsibilities

- Web: use the shared timestamp parser for charts, live/operations status and
  school-hour classification. Compute PV calendar keys in Athens, even for UTC
  source timestamps.
- iOS: use `MonitoringDate.parseTimestamp` for environmental, energy and Home
  measurements; preserve Athens display/calendar formatters.
- VPS/Render: reject malformed environmental upstream timestamps with 502,
  preserve explicit offsets in device requests and use aware UTC export keys.
  Athens/UTC export columns describe the same instant.
- PV ingestion: injected clocks must carry an offset; preserve provider epoch
  conversion and Athens target-date logic.
- UPAT rollup initialization: compare the TIMESTAMPTZ cutoff with `NOW()`
  without stripping its timezone.

## Requests and verification

Request builders preserve explicit instants and calendar intent. The
[retained low-level request compatibility](TIME_PERSISTENCE.md#low-level-readers)
is distinct from strict measurement parsing; endpoint-specific windows and
limits live in their public contracts.

Check aware inputs, explicit output offsets, DST windows, calendar exports and
consumer compatibility before release. Use local fixtures for source tests and
record live consumer acceptance separately; a build alone is not device/UI proof.

## Recovery

Restore only a consumer release compatible with the current API, timestamp
representation and endpoints. Preserve the
[persistence/recovery contract](TIME_PERSISTENCE.md#validation-and-recovery)
and [compact storage requirements](shelly-compact/RECOVERY.md).
