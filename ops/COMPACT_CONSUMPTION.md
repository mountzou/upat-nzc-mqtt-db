# Compact consumption API v1

The existing authenticated school-room hourly endpoint accepts `format=compact`; omitted or `format=legacy` keeps the legacy response. The compact marker is `consumption.compact.v1`. School ACL and the canonical school/room/phase series plan are shared by both formats.

The response retains hourly total Wh, timestamps and per-hour observed-series count. Period summary metrics are supplied in kWh (CO2 in kg), with a single period device/phase breakdown and coverage counts. No hourly device dictionaries are constructed for compact output. Both serializers use the same series accumulator.

Coverage counts fully contained UTC hours and respects the existing school-hours policy, including DST intervals. `complete_hour_count` means all expected catalog series have a non-null stored hourly energy value; it does not guarantee raw-sample completeness within that hour. Partial hours contribute their observed energy and remain identifiable through `observed_series_count`. No observations means null summary values, not measured zero. Entirely absent series have null period energy. Valid measured zero stays zero.

`fetch_shelly_hourly_energy_rows(..., preserve_missing=True)` is an internal query option used by compact. The public legacy device API still serializes missing values as before. SQL queries/tables, ingestion and database schema are unchanged. The bound is 90 days; invalid nonfinite/negative compact measurements fail closed.

The iOS live transport explicitly requests compact, validates its version/scope/coverage, consumes server overview/breakdown, and keys its shared session cache by contract plus school/room/interval. Legacy remains available for existing clients.

## Validation and activation

On 2026-09-06 the local VPS suite passed 136 tests plus 9 subtests; 15 optional tests require disposable PostgreSQL and were skipped locally. New tests cover numerical parity, legacy defaults, ACL, phase/whole-school plans, NULL/zero/missing values, DST, window clipping, invalid measurements and range bounds.

The candidate extends the running production image with six scoped Python files, preserving dependencies and environment. Read-only acceptance checks canonical historical data before an API-only recreation. Public HTTPS health is intentionally not exposed (`/health` returns 404); internal health is 200. Two early activation attempts were automatically rolled back because the acceptance script first expected a public health 200 and then attempted JSON decoding of the proxy's plain-text 404. The corrected harness was preflighted on the existing public API before retrying and preserves this proxy restriction. No Caddy configuration change is required.

Rollback image: `schoolheroz-compact:rollback-20260906`. Private source backup, hash manifest, candidate/HTTPS acceptance and container-state evidence: `/root/schoolheroz-compact-20260906`. API-only operations use `docker compose -f docker-compose.prod.yml up -d --no-deps --no-build api`; PostgreSQL, Caddy, MQTT and ingestor identities/start times must remain unchanged. Never run a full-stack recreation for this contract.

Measured JSON body sizes: School 10 whole-school 24h 21,978 → 5,046 bytes (77.0%); 168h 150,899 → 23,854 bytes (84.2%). Real-data totals and breakdown match within floating-point precision. API latency varied; no consistent speedup or phone frame-time improvement is asserted.

The canonical dirty checkout also contains independent timezone improvements. Scoped three-way integration preserved those edits; its combined suite passed 155 tests and 9 subtests, with 15 optional PostgreSQL skips. Compact normalizes both naive legacy UTC and aware UTC bounds through the existing `as_utc` helper.

Final HTTPS activation accepted at 2026-09-06 13:37:57 UTC. Live image: `sha256:fd360e266f1ee4e09e7894a8c67cf6077555d222c15e454329fb42587e27289b`. Protected PostgreSQL/Caddy/MQTT/ingestor IDs, start times and restart counts matched the pre-stage snapshot. The temporary candidate was stopped; rollback is retained.
