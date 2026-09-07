# Environmental history compatibility retirement

## Status and canonical reference

The production API switched to strict mode on 7 September 2026, with verified
web and iOS Simulator consumers. Its old 24-hour Python calculation was removed
in the same rollout. See `../backups/environmental-24h-rollout-20260907/REPORT.md`.

The subsequent API-only rollout on 7 September 2026 removed the remaining
disabled public compatibility code from the image and aligned the four affected
VPS source paths. See `../backups/environmental-final-rollout-20260907/REPORT.md`.
[ENVIRONMENTAL_HISTORY.md](ENVIRONMENTAL_HISTORY.md) is the current public query,
response, SQL, calendar and missing-data contract.

## Removed

- `history/compatibility.py`: alternate request parser, legacy request type,
  dispatcher and environment setting.
- `history/legacy_reader.py`: old rolling one-hour Python mean and its helpers.
- The route's alternate legacy response/exception branch and unused serializers.
- `ENVIRONMENT_HISTORY_LEGACY_COMPAT` from `.env.example` and both base Compose files.
- Tests exclusive to accepted legacy requests; permanent strict rejection and
  canonical consumer regression tests remain.

The response schema, validation, canonical header/log and SQL calculation are
unchanged. Retired `aggregate`, `rolling_*` and `bucket_*` query controls still
return 422. Bare requests retain the fixed 24-hour default. Old environment
values, even true, are ignored by the cleaned runtime; they cannot restore the
removed implementation.

## Preserved dependencies

The internal general history reader remains active for home's latest-minute
snapshot and environmental insights. Protected low-level backend/research
contracts are unchanged. Their `aggregate` arguments are not public compatibility
leftovers and must not be deleted by a keyword search. The canonical 24-hour
home adapter and ordinary/streaming home response envelopes are unchanged.

## Deployment and rollback boundary

The final image derives from the exact preceding strict image, replaces two
runtime files, and explicitly deletes the two retired modules and their cached
bytecode. Full image/source hash checks confirmed this four-path boundary.
Only the API container was recreated; other containers, Caddy, database and MQTT
collection were unchanged. No consumer deployment or database migration occurred.

The subsequent production Compose consolidation is active. The API now uses
only `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml` and the existing protected
`.env`; the obsolete flag is not mapped into the API. Historical overlays,
including the final private `environment: !override` snapshot, remain archived
but are no longer active inputs. Future variable changes do not require updating
that historical snapshot. See [PRODUCTION_COMPOSE.md](PRODUCTION_COMPOSE.md).

For historical environmental-image rollback, restore the matching historical
base/configuration as well as the selected image; do not assume the now-updated
base is identical to the one used in the old report.

The saved rollback overlay restores the preceding strict image and its false
flag. The image archive and configuration/source backup were verified before
activation. No rollback was needed. Returning to that image preserves strict
public behavior; it does not re-enable legacy requests.
Restoring accepted legacy requests requires the saved canonical-capable bridge
image plus its corresponding configuration; merely changing the flag on the
cleaned image cannot do that. No consumer or database rollback is required to
return to the preceding strict API.

## Historical evidence

- `../backups/environmental-compatibility-20260907/REPORT.md`: bridge preparation.
- `../backups/environmental-rollout-20260907/REPORT.md`: initial API/consumer rollout.
- `../backups/environmental-strict-preparation-20260907/REPORT.md`: strict rehearsal.
- `../backups/environmental-24h-rollout-20260907/REPORT.md`: strict activation and acceptance.
- `../backups/environmental-cleanup-20260907/REPORT.md`: final local code cleanup.

- `../backups/environmental-final-rollout-20260907/REPORT.md`: final API-only deletion rollout.
