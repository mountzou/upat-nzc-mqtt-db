#!/bin/sh
# Compatibility entrypoint; the systemd service owns the writer lock and image.
set -eu
[ "$#" -eq 0 ] || { echo 'energy aggregator: no arguments accepted' >&2; exit 2; }
exec /usr/bin/systemctl start upat-aggregate-energy.service
