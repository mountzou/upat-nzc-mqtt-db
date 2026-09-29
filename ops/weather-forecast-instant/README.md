# Weather forecast UTC-instant rollout boundary

This change is source preparation. Neither migration is run automatically by
Compose or `db/init.sql` against an existing database. Production PostgreSQL is
owned by `upat-postgres-volume.service`, outside Compose. Review a current
backup and its restore test before any database mutation.

1. Inspect the live table, indexes, collector image, API image, and active
   scheduler read-only. The prepare migration rejects ambiguous legacy local
   hours because their original UTC instant cannot be reconstructed reliably.
2. Apply `db/migrations/018_weather_forecast_instant_prepare.sql` separately.
   It backfills existing unambiguous rows, adds UTC-instant uniqueness, and
   retains the old local-time unique key. The new column remains nullable:
   the currently deployed collector can keep inserting rows with NULL instants
   while this preparation is reviewed. Keep the old API active at this stage.
3. Build and review the new API and collector images, then stop and drain the
   old scheduled collector writer at a release checkpoint. Confirm no old
   invocation can run during finalization or the targeted image rollout.
4. Apply `db/migrations/019_weather_forecast_instant_finalize.sql`. It refuses
   ambiguous rows written since preparation, backfills remaining NULLs, makes
   the instant mandatory, and drops the legacy local-time unique key in one
   transaction. If it fails, the old schema contract remains available.
5. Release and verify the new API and weather collector as targeted services,
   preserving the systemd-owned PostgreSQL container, then resume the scheduler.
   The collector requests UNIX timestamps from Open-Meteo and writes both the
   local display time and its unique UTC instant. The API retains `timestamp`
   and adds `instant`. The old collector cannot be used as a rollback writer
   after finalization; retain a compatible new collector image as the rollback
   target.

The separate EnergyPlus backend must still map these unique forecast instants
into its fixed-offset EPW and produce 23/25 elapsed-hour day-ahead profiles.
This database change alone does not make DST-day simulation complete. Do not
merge or release the recorder PR on that assumption.
