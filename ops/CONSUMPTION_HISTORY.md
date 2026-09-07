# Consumption history contract

The canonical API and web consumer were deployed on 2026-09-06; iOS build 5 was validated and installed on the Simulator. On 2026-09-07 the retired room-hourly route and its legacy/compact serializers were removed from the VPS API. The shared web/iOS checkout was also reconciled with the consumption release, preserving unrelated local work. Current cleanup evidence and rollback instructions are recorded in `backups/consumption-cleanup-20260906/REPORT.md` (the work began before midnight). A physical iPhone with an older consumer needs build 5 to load consumption.

```http
GET /energy/consumption/history?school_id=school_10&start_date=2026-09-01&end_date=2026-09-05&interval=1h
GET /energy/consumption/history?school_id=school_10&room_id=teachers&start_date=2026-09-01&end_date=2026-09-05&interval=day
GET /energy/consumption/history?school_id=school_10&start=2026-09-01T09:00:00Z&end=2026-09-02T09:00:00Z&interval=1h
```

## Parameters and access

Authenticated user JWT; school authorization is enforced before source resolution or cache lookup.

| Parameter | Contract |
| --- | --- |
| `school_id` | Required accessible school. |
| `room_id` | Optional room. Omit for the school total; `all_rooms` is not a public alias on this route. |
| `start_date`, `end_date` | Inclusive Athens calendar dates, 1–90 days. Both required together. |
| `start`, `end` | Alternative offset-aware instants, inclusive start/exclusive end, whole-hour aligned. Supports rolling completed-hour app windows. Cannot be mixed with dates. Maximum 90 elapsed days plus the autumn DST hour. |
| `interval` | Default `1h`; integer hour multiples through `168h`, or `day`. `60m` normalizes to `1h`. Reject `30m`, `90m` and non-integer hour durations. |

Unknown parameters and duplicate query keys return 422. There is no `metric`, `aggregate`, `working_only`, `format`, device selector or timezone input. Energy is always additive kWh. Fixed intervals follow the shared UTC origin; calendar `day` follows Europe/Athens and therefore includes 23/25-hour DST days. Bins touching the request boundary are clipped and return their actual bounds. Hourly stored energy is never prorated.

## Meter topology

The existing energy device catalog defines room and phase assignments. The separate `energy_consumption_assumptions.json` records the explicit project assumption for school_10.

* A single three-phase meter assigned to `all_rooms` is the authoritative central school meter. School-total queries select its phases only; other plugs and submeters are excluded. Missing central readings never trigger fallback to submeters.
* Without a central meter, school queries select every independent plug and every phase once. Device total columns are not added to phase values. For school_10 this is five plugs plus nine phases (14 source series), presented as an assumed school total.
* Room queries select plugs assigned to the room and phase channels whose target room matches, including channels on a device whose base room differs.
* Phase channels with the same device, target room, load type and display label form one displayed load; their energy is summed on the server.
* Multiple central meters or duplicate device IDs fail closed with 409. A scope with no assigned meters returns 404.

`source_basis` is `central_meter` or `submeter_sum`. `measurement_scope` distinguishes `whole_school`, `assumed_school_total`, and `metered_loads`. This is independent of temporal `coverage`: a complete set of hourly records for an AC plug does not establish full-room measurement coverage.

## Response and quality

`contract=consumption.history.v1`, `unit=kWh`, `timezone=Europe/Athens`. Output instants are unambiguous UTC `Z` timestamps. `fetched_at` identifies when the prepared result was built.

* `points`: contiguous requested buckets, energy, equivalent CO2, outside-school-hours energy, quality and observed/expected source-hour counts.
* `summary`: observed total energy, CO2, observed-hour mean/peak, maximum requested bucket, and school-hours energy/share. The average is a descriptive server statistic; aggregation of energy always uses sum.
* `breakdown`: load metadata once per response, period energy, source-hour counts and quality. Each point's `load_energy_kwh` and `load_equivalent_co2_kg` vectors use this exact order. Clients do not group phases or reconstruct period totals.
* `days`: Athens calendar totals, school-hours split and per-day coverage.
* `hourly_profile`: mean energy for each Athens clock hour and its share of the mean profile. Both occurrences of an autumn repeated hour are observations; spring's missing clock hour is not fabricated.

An observed zero remains zero. Entirely missing buckets are null. Partial buckets contain the observed subtotal and are marked partial; values are never scaled up for missing sources or hours. The summary's energy is also an observed subtotal if coverage is partial. Ratios with a zero energy denominator are null.

Coverage measures valid persisted **hourly source records**, not raw MQTT sample completeness. Existing rollups do not provide raw sample coverage; this change does not invent that information. NULL, negative, non-finite, off-grid or malformed-duration source rows do not contribute. The existing Mon–Fri 08:00–14:59 Athens school-hours policy is reused.

## Efficiency and compatibility

One parameterized SQL round trip reads only the selected devices/channels and bounded time range in `shelly_plug_hourly_energy` / `shelly_pro3em_hourly_energy`. The existing primary-key prefixes support device/time filtering. Raw power and MQTT tables are not scanned. Bounded in-process caching retains at most eight prepared responses for 60 seconds and coalesces identical concurrent reads. Keys include topology, scope, bounds, interval and emissions metadata. Browser responses use `Cache-Control: private, no-store`.

The old `/energy/schools/{school_id}/rooms/hourly-energy` route is retired and returns 404, including requests using `format=compact`. The old Wh room response schemas, compact builder, room-series grouping/serialization helpers and compact-only NULL-preservation switch were removed. Device telemetry and energy insights remain separate contracts. The legacy `pro3em-energy` power estimate was retired from the live VPS API on 2026-09-07. The internal hourly device query remains necessary for insights and research exports. Above-baseline and PV-alignment displays still compare separate insight/production contracts; this batch moves consumption values, totals, load grouping and hourly profiles to the new service.

The web Home, consumption screen and PV demand-alignment consumer use the new route. The iOS transport uses the new route and validates scope, units, bounds and hourly quality before adapting to existing chart models. Its server metadata supplies totals, metrics, daily calendar energy and breakdowns; the older in-memory fixture/presentation models are retained to preserve existing repository and paging behavior. No new endpoint fallback is performed by either production consumer.

## Completed rollout order

1. Reconfirm current VPS image, catalog and health; back up current API source/image. Use the isolated additive candidate, not the unrelated dirty API tree.
2. Roll out API only, keeping existing routes and tables. Validate school_10, school_22 central selection, rooms, interval/day/DST, authorization and missing-data behavior. Caddy's existing `/energy/*` proxy already covers the new route.
3. Publish the tested web consumer and release/install the iOS consumer after API availability. Render has no new consumption runtime caller in the inspected baseline; no backend release is required solely for this contract.
4. The subsequent user-requested cleanup retired the room-hourly route after consumer inventory, source reconciliation, a read-only candidate and unchanged-result verification. Live device telemetry and insights were retained.

No database migration or MQTT pause was needed. Only the API container was replaced for the retirement rollout. The immediately preceding API image still supports the canonical endpoint and can restore the legacy route if rollback is needed. Rolling the web back to a consumer using the retired route requires restoring API compatibility first; do not use the earlier web-only rollback instructions without this dependency.

The internal counter accounting policy is documented in [SHELLY_COUNTER_ENERGY.md](SHELLY_COUNTER_ENERGY.md); it does not change the public history response.
