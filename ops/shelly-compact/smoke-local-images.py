#!/usr/bin/env python3
"""Exercise the actual candidate images against the disposable local fixture only."""

import argparse
import hashlib
import json
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def docker(*args):
    return subprocess.check_output(
        ["docker", "--context", "desktop-linux", *args], text=True
    ).strip()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--api-image", required=True)
    p.add_argument("--ingestor-image", required=True)
    p.add_argument("--receipt", required=True)
    a = p.parse_args()
    for item in (a.api_image, a.ingestor_image):
        if not item.startswith("sha256:"):
            p.error("Use immutable local image IDs")
    network = "codex-shelly-migration-20260916-host"
    db = "codex-shelly-migration-20260916"
    info = json.loads(docker("inspect", db))[0]
    assert network in info["NetworkSettings"]["Networks"]
    common = [
        "--platform",
        "linux/amd64",
        "--network",
        network,
        "-e",
        f"POSTGRES_HOST={db}",
        "-e",
        "POSTGRES_DB=compact_fixture",
        "-e",
        "POSTGRES_USER=codex_backup_verify",
        "-e",
        "POSTGRES_INTERNAL_PORT=5432",
    ]
    # Fixture schema is created/reset by test_shelly_compact_postgres.py.
    # Use the real MQTT callback, including devices/raw/counters transaction.
    code = """import json
from types import SimpleNamespace
from datetime import datetime
import main
stamp=datetime.fromisoformat('2026-09-15T12:00:00+00:00').timestamp()
for i,value in enumerate([286.1,286.7,286.6,286.4]):
 payload={'apower':value,'aenergy':{'total':100+i,'minute_ts':stamp},'ret_aenergy':{'total':0}}
 main.on_message(None,None,SimpleNamespace(topic='shellyplugsg3-imagefixture/status/switch:0',payload=json.dumps(payload).encode()))
"""
    docker(
        "run",
        "--rm",
        *common,
        "-e",
        "SHELLY_MEASUREMENTS_WRITE_MODE=dual",
        "-e",
        "VERBOSE_LOGGING=false",
        "--entrypoint",
        "python",
        a.ingestor_image,
        "-c",
        code,
    )
    results = {}
    token = "local-fixture-only-" + "x" * 40
    for mode in ("legacy", "compact"):
        name = "codex-shelly-api-fixture-" + mode
        docker(
            "run",
            "-d",
            "--name",
            name,
            *common,
            "-p",
            "127.0.0.1::8000",
            "-e",
            f"SHELLY_MEASUREMENTS_READ_STORAGE={mode}",
            "-e",
            "SHELLY_MEASUREMENTS_ROUNDING=decimal_1",
            "-e",
            f"DATA_SERVICE_TOKEN={token}",
            a.api_image,
        )
        try:
            port = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"][
                "8000/tcp"
            ][0]["HostPort"]
            base = "http://127.0.0.1:" + port
            for _ in range(40):
                try:
                    with urlopen(base + "/health", timeout=2) as r:
                        assert r.status == 200
                    break
                except (URLError, TimeoutError, ConnectionError):
                    time.sleep(0.5)
            else:
                raise RuntimeError("Local API did not become healthy")
            endpoints = [
                "/shelly/device/shellyplugsg3-imagefixture/latest?limit=2",
                "/shelly/device/shellyplugsg3-imagefixture/history?start=2026-09-15T00:00:00Z&end=2026-09-16T00:00:00Z&interval=1m",
                "/shelly/device/no-fixture-device/latest?limit=1",
                "/internal/data/shelly/device/shellyplugsg3-imagefixture/latest?limit=2",
            ]
            answers = []
            for path in endpoints:
                with urlopen(
                    Request(base + path, headers={"Authorization": "Bearer " + token}),
                    timeout=10,
                ) as r:
                    answers.append(json.load(r))
            assert answers[0]["items"][0]["measurements"]["apower"]["value"] == 286.5
            assert answers[0] == answers[3]
            assert answers[2]["count"] == 0
            try:
                urlopen(base + endpoints[-1], timeout=10)
                raise AssertionError("Internal endpoint accepted missing credentials")
            except HTTPError as error:
                assert error.code == 401
            results[mode] = answers
        finally:
            docker("stop", "--time", "10", name)
            docker("rm", name)
    assert results["legacy"] == results["compact"]
    receipt = {
        "status": "PASS",
        "api_image": a.api_image,
        "ingestor_image": a.ingestor_image,
        "checks": [
            "real MQTT callback in dual mode",
            "legacy/compact HTTP equality",
            "one-decimal tie",
            "empty result",
            "internal service token accepted",
            "missing service token rejected",
        ],
        "response_sha256": hashlib.sha256(
            json.dumps(results["compact"], sort_keys=True).encode()
        ).hexdigest(),
    }
    with open(a.receipt, "w") as f:
        json.dump(receipt, f, indent=2)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
