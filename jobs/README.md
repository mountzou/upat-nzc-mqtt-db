# Scheduled jobs

This directory is the home for application workloads that run on a schedule. Each migrated workload gets its own `jobs/<job-name>/` directory containing its implementation, tests, and versioned systemd service/timer definitions.

Move jobs here one at a time. A migration is complete only after the deployed scheduler points to the new implementation, the previous schedule is removed, and the expected output is verified.

Existing workloads remain in their current locations until they are migrated. Continuously running services do not belong here.
