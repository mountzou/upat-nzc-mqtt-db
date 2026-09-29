# School 10 — real-data local notification validation

> Historical validation: predates the aggregate-only reader and the final equal-sensor Teachers Office rule. Not final-version acceptance evidence.

Production data was read through read-only PostgreSQL transactions. Notification generation and API checks ran in a disposable local PostgreSQL 15 database. No production writes or scheduler changes were performed.

Source captured: 2026-09-09T15:36:27.644402+00:00. Export: 58,434 CO2/PM2.5 samples, five devices, four rooms.

The local room catalog and thresholds matched the live VPS catalog/policy. Each room mean was independently calculated from raw samples with Python math.fsum and compared with the sample-weighted output of the existing live canonical history reader. The persisted notification set and evidence matched the independent strict-threshold expectations.

## Hourly: 2026-09-09T17:00:00+03:00 to 2026-09-09T18:00:00+03:00

| Room | Metric | Average | Threshold | Samples | Notify |
| --- | --- | ---: | ---: | ---: | --- |
| Computer Classroom | co2 | 507.5667 | 750 | 240 | No |
| Computer Classroom | pm25 | 6.0000 | 10 | 240 | No |
| Event Hall | co2 | 547.6292 | 750 | 240 | No |
| Event Hall | pm25 | 10.0625 | 10 | 240 | Yes |
| Library | co2 | 522.0812 | 750 | 234 | No |
| Library | pm25 | 5.0427 | 10 | 234 | No |
| Teachers Office | co2 | 750.4790 | 750 | 476 | Yes |
| Teachers Office | pm25 | 5.7143 | 10 | 476 | No |

Persisted notifications: **2**. Repeated run: `already_checked`, with no additional notifications.

## Daily: 2026-09-08T00:00:00+03:00 to 2026-09-09T00:00:00+03:00

| Room | Metric | Average | Threshold | Samples | Notify |
| --- | --- | ---: | ---: | ---: | --- |
| Computer Classroom | co2 | 490.1695 | 800 | 5687 | Yes |
| Computer Classroom | pm25 | 2.0032 | 15 | 5687 | Yes |
| Event Hall | co2 | 557.3467 | 800 | 5408 | Yes |
| Event Hall | pm25 | 0.2441 | 15 | 5408 | Yes |
| Library | co2 | 562.4444 | 800 | 5662 | Yes |
| Library | pm25 | 2.6116 | 15 | 5662 | Yes |
| Teachers Office | co2 | 774.0101 | 800 | 11270 | Yes |
| Teachers Office | pm25 | 2.5647 | 15 | 11270 | Yes |

Persisted notifications: **8**. Repeated run: `already_checked`, with no additional notifications.

## API and interpretation

- `school_10`: 10 notifications (2 hourly + 8 daily).
- `school_3`: no notifications and 404 when attempting to mark a school 10 notification as read.
- Teachers Office combines both assigned devices into one mean and one notification per metric/period.
- All four rooms had valid samples for both metrics in both periods; this real-data snapshot does not exercise missing-data behavior (covered by automated fixtures).
- The two hourly breaches are small but strictly above the agreed thresholds; comparisons use unrounded values.
- This is retrospective evaluation of measurements available at capture time. It does not prove that exactly the same data had arrived at the original scheduled 00:10/18:00 runs. The new scheduler is not deployed.
- The disposable local database was used only for this validation. Detailed persisted evidence is in `results.json`; the raw export is deliberately kept outside the repository.

Source snapshot SHA-256: `94863b07f4d5c466fae8db070bd53f8f840123e68ca53698c087706931944f9e`.
