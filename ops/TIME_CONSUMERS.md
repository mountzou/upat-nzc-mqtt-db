# Telemetry consumers after migration 014

## Release state — 6 September 2026

The consumer cleanup is live on the VPS monitoring API, Render backend and
Vercel web application. SchoolHeroZ 1.0.0 build 2 was installed and launched on
the connected iPhone. The PV scheduled consumer points to the updated immutable
image for its next normal run; no provider call was triggered for verification.
Migration 014 remains live. No new migration, MQTT collection pause,
PostgreSQL/TTN/Shelly/Mosquitto restart, or Caddy change was performed.

Deployment receipt: `backups/time-consumer-rollout-20260906/REPORT.md`.
The rollout used isolated production baselines, preserving unrelated local and
VPS disk changes. In particular, unreleased compact-consumption code was not
included in the deployed API image or iOS build.

## Contract

Measurement timestamps represent explicit instants. JSON values include `Z` or
a numeric UTC offset. Parsers reject offset-free measurements instead of
assuming UTC, Athens or the device/browser timezone. Python consumer windows
remain timezone-aware and requests preserve their offsets.

`Europe/Athens` remains the fixed application calendar and display timezone.
Date picker values, school hours, calendar-day aggregation and daily export
boundaries still use Athens IANA rules. A local day can have 23, 24 or 25 hours.
The two occurrences of `03:00` on 25 October 2026 remain distinct instants.
Sorting uses instants rather than the lexical order of timestamp strings.

### Changed consumers

- Web: common timestamp parser, environmental and energy chart points/labels,
  live status through its existing shared parser, Operations timestamps, solar
  observed-quarter-hour points and school-hour classification. PV daily keys
  are computed in Athens, including when the source timestamp is in UTC.
- iOS: `MonitoringDate.parseTimestamp` is the common strict measurement parser.
  IndoorEnvironment and the energy/Home consumers reuse it. The four legacy
  offset-free `DateFormatter` formats have been removed from both old parsers.
- VPS monitoring and Render backend: strict instant helpers and environmental
  measurement DTOs; malformed environmental upstream timestamps yield 502.
  Explicit UTC offsets survive device/Shelly request serialization. Research
  environmental, energy and PV exports retain aware UTC bucket keys and expose
  both Athens and UTC representations of the same instant.
- PV ingestion transformation: injected collection/current clocks must carry
  an offset. Provider epoch conversion and Athens target-day logic remain.
- UPAT rollup initialization: the cutoff uses `NOW()` as a timestamptz instead
  of stripping its zone before comparison with measurements. This prevents a
  session-timezone shift in the lookback. The initializer was tested locally
  and was not executed on production.

### Request compatibility and deliberately retained rules

Monitoring history and energy `start`/`end` query parameters require an explicit
offset; invalid requests receive 422 rather than an implicit interpretation.
The current web and iOS request builders already send offsets.

The low-level `/upat` and `/shelly` API's existing legacy request-bound parsing
in `api/schemas.py` remains an explicit compatibility boundary. Consumers no
longer depend on it. Removing that public compatibility contract is a separate
API versioning decision.

Weather/PV forecast civil timestamps with their declared timezone, simulation
calendar labels and old on-disk simulation-retention metadata are outside the
measured-telemetry persistence migration. Their domain-specific interpretation
remains. UTC normalization, Athens display conversion and timestamp validation
are still required; none is a repair for ambiguous measurement persistence.

## Verification

- API: 178 tests and 9 subtests passed, including integration with a disposable
  local PostgreSQL 15 schema and explicit rejection of ambiguous timestamps.
- Backend: 101 tests and 5 subtests passed across device/Shelly consumers,
  exports, insight generation and the indoor-environment AI executor (network disabled).
- Web: 474 tests passed; seven unchanged LoginView tests fail because their
  fixture lacks AppearanceProvider and were excluded from the final suite.
  A separate run with `TZ=America/Los_Angeles` passed all 24 calendar/consumer
  checks. The production build also passed.
- PV transformation: 7 tests passed. A disposable PostgreSQL test verified both
  rollup initialization tables exclude a reading outside the 48-hour cutoff
  when the database session is Europe/Athens.
- iOS: 149 tests passed in the signed Simulator suite.
  Initial unsigned tests encountered a missing Keychain entitlement;
  the signed run resolved it. The existing 20 ms cancellation test was also
  sensitive to parallel suite timing. Its fixture now waits for both requests
  to start, with a one-minute test timeout, rather than sleeping for 20 ms.
  Baseline comparison was performed in an isolated source copy; application
  cancellation behavior is unchanged.
- Live read-only contract: 12 HTTP checks passed. The checked UPAT, Shelly and
  PV responses contained 79 parsed timestamps and zero offset-free timestamps.
  This validates the producer prerequisite, not deployment of this patch.

## Review and rollback artifacts

Private, gitignored receipts are in
`backups/time-consumer-cleanup-20260906/`:

- `baseline.json` and `before/`: hashes and source snapshots taken before edits.
- `changed-files.json`: exact per-file changes for this task.
- `mqtt.patch`, `energyplus.patch`: scoped patches relative to each repository.
- Test logs, xcresult summaries, live HTTP checks and SQL cutoff verification.

The repositories contained unrelated work at the start. Review and deploy only
these scoped changes. Never revert the full working tree. For a local reversal,
first verify the current file hashes, then check the reverse patch with
`git apply --check -R` from the corresponding repository. Retain intervening
edits rather than overwriting them from the snapshot.

At deployment, keep the previous API/backend image or release, previous web
release and previous iOS build available. Roll back consumer code if needed;
do not roll back migration 014 for this cleanup. The previous consumers already
accept explicit offsets. Updating this code requires no PostgreSQL/MQTT restart
and no new data migration.

## Production rollout — 6 September 2026

- VPS API: `schoolheroz-time-consumer-api:20260906`, image
  `sha256:a985f67fac58f7560d4962c6d277421104774def07af54676567c20ca8ae6227`.
  Twelve acceptance requests passed, with 79 explicit timestamps, plus 422
  rejection and autumn repeated-hour checks. All five protected containers
  kept their IDs, start times, image IDs and restart counts. Post-rollout UPAT
  and Shelly persisted readings were approximately 12 seconds old.
- Render: commit `2a9839e17d3e858aa513e655adf59ecfe670b6d2`, deployment
  `dep-daentd6q1p3s73d7t4kg`, Live. Ninety-six read-only checks passed and all
  eight stored completed simulations retained identical metadata hashes.
- Web: commit `0e864afb26c81cce8190002e940d199041631354`, deployment
  `dpl_Eq6RRBrhdof1ApginU3aC5AbmHgs`, Ready with production aliases assigned.
  The exact release passed 481 tests and its production build. The known seven
  LoginView fixture failures were excluded; initial parallel-suite timeouts
  disappeared when concurrency was bounded at four workers.
- iOS: exact production-derived source passed 144 Simulator tests (157 dynamic
  executions). The signed device build 2 was installed, verified by read-back,
  and reinstalled with user approval after an intervening build 1 installation.
  Final read-back confirmed build 2, a successful launch, and a running process
  whose bundle path matched the installation receipt. A full physical-device
  visual acceptance test has not been performed.
- PV: seven offline transformation tests passed against the new image;
  timer schedule and cooldown were preserved. The SQL initialization helper was
  updated on disk but was not executed.

The first activation was unnecessarily rolled back because the rollout script
probed `/health` through Caddy, which intentionally returns 404 for that path.
Both old and new API images were internally healthy. The probe was corrected
to use internal health and real public API routes; the retry passed. Only the
API was recreated during these attempts; collection stayed active.

Rollback artifacts and executable API rollback are retained under
`/opt/schoolheroz-time-consumers-20260906T1445` on the VPS. The private local
rollout directory includes both signed iOS app archives, exact patches,
per-file hashes, test logs and provider deployment IDs. Revert consumer code
only; do not roll back migration 014.
