# Compact-only images with the production runtime

Build the requested source cleanup on the exact inspected production images.
Only the reader/writer source files are overlaid; Python, packages, command and
entrypoint remain inherited. Pass BASE_IMAGE explicitly and label the resulting
image with the committed revision before changing production Compose references.

The initial rebuild from python:3.12-slim refreshed unpinned Python/framework
packages. Those images remain local test artifacts and are superseded for rollout
by these production-runtime overlays.

Prepare a uniquely named local base reference from each inspected image archive.
Verify the archived config identity against the live image, then build from this
repository root with the corresponding Dockerfile and --platform linux/amd64.
No Dockerfile here runs pip or changes dependencies.

The bootstrap SQL is for fresh databases. The existing production compact schema
already matches it; do not execute init.sql or migration 014 during this rollout.
Production application changes remain limited to API and Shelly ingestor, with the
existing PostgreSQL, Caddy and other services retained.
