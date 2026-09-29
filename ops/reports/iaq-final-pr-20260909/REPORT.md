# Final IAQ notification PR preparation — 9 September 2026

## Production source reconciliation

The two local commits `96ac96c` and `8db8450` were absent from origin/main but describe the deployed hourly-energy reader. All five changed runtime API files matched the live container byte-for-byte (SHA-256). They were cherry-picked onto the notification branch as `0c37147` and `c8f8c80`, preserving the published branch history. No main branch was changed.

Verified production API image: `sha256:f2cc6719c8f722987265b13417c170eb74c3a5518db20e273f2ca7cbd0389089`. The Compose image pin remains the already deployed baseline; deployment of notifications requires a separately approved new image/migration/timer rollout.

## Final School 10 replay

Capture: 2026-09-09T15:57:58.063344+00:00. Production transaction read-only: `on`. Only **250 hourly aggregate rows** were exported (10 previous-hour rows and 240 previous-day rows); no raw measurements.

Previous hour: 9 September 2026 17:00–18:00 Athens, evaluated locally as the 18:10 run. Previous day: 8 September 2026, evaluated locally as the 9 September 00:10 run. The snapshot was read later; this validates the final rules against stored aggregates, not historical scheduler delivery or data availability at the original scheduled time.

For each sensor the independent Python reference uses the stored hourly mean, or the sample-weighted combination of hourly aggregates for daily checks. Room `teachers` takes the simple mean of the two sensor means. Persisted SQL results were compared against that independent reference.

| Period | Room | Metric | Mean | Threshold | Notification |
| --- | --- | --- | ---: | ---: | --- |
| hourly | computerclassroom | co2 | 507.56667 | 750 | No |
| hourly | computerclassroom | pm25 | 6.00000 | 10 | No |
| hourly | eventhall | co2 | 547.62917 | 750 | No |
| hourly | eventhall | pm25 | 10.06250 | 10 | Yes |
| hourly | library | co2 | 522.08120 | 750 | No |
| hourly | library | pm25 | 5.04274 | 10 | No |
| hourly | teachers | co2 | 749.69244 | 750 | No |
| hourly | teachers | pm25 | 5.72034 | 10 | No |
| daily | computerclassroom | co2 | 490.16951 | 800 | Yes |
| daily | computerclassroom | pm25 | 2.00317 | 15 | Yes |
| daily | eventhall | co2 | 557.34671 | 800 | Yes |
| daily | eventhall | pm25 | 0.24408 | 15 | Yes |
| daily | library | co2 | 562.44437 | 800 | Yes |
| daily | library | pm25 | 2.61162 | 15 | Yes |
| daily | teachers | co2 | 773.44076 | 800 | Yes |
| daily | teachers | pm25 | 2.56642 | 15 | Yes |

**Result: 1 hourly elevated PM2.5 notification and 8 daily good notifications.** Teachers hourly CO2 is 749.69244 ppm, below 750: no notification. This supersedes the earlier pooled-sample result of two hourly notifications.

Actual PostgreSQL persistence matched the expected set. Repeated jobs added no duplicates. School 10 saw 9 entries; marking one read reduced unread count by one. School 3 saw none and received 404 for a School 10 read mutation.

## Validation

- Combined branch API suite: **495 passed, 12 subtests passed, 4 skipped**. Notification, monitoring and environmental-history PostgreSQL fixtures enabled on disposable local databases.
- Four skips are optional legacy database tests gated by `OPERATIONS_RUN_DB_TESTS` / `UPAT_RUN_DB_TESTS`; they were not executed.
- Combined candidate API Docker image built successfully.
- Found and fixed the missing Caddy public allowlist entries for `/notifications` and `/notifications/*/read`.
- Local Caddy 2.8 → candidate API checks: unauthenticated GET/PATCH return 401; both browser CORS preflights return 200 for schoolheroz.com; unrelated route remains 404. Only test listener/upstream addresses differ from the repository Caddyfile. Live Caddy was not changed.
- `git diff --check` passed.

## Boundaries

No production migration, container replacement, Caddy reload, scheduler activation, branch merge or frontend changes were performed. The notification PR is prepared for human review. Timers remain templates; live scheduled delivery is not yet verified.
