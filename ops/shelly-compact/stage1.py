#!/usr/bin/env python3
"""Guarded, explicit production stage 1. Never restarts PostgreSQL or the API."""

import argparse
import copy
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
import subprocess
import time
from pathlib import Path

ROOT = Path("/opt/upat-nzc-mqtt-db")
STAGE = Path(__file__).resolve().parents[2]
RECEIPTS = STAGE / "receipts"
IMAGE = "sha256:f92a082e0fbbb5cbbd4f3ab922b589df1ca22b403d4f291bbd8f30ace3308d7b"
APP_COMMIT = "586ce99272372fdc4b6107a561017d5c40ead562"
OLD_IMAGE = "sha256:540f7d5f5d4e277593a3fea2e9bccd6dc96a3817483fd2fa5d2d63ce39f984dd"
OLD_COMPOSE = "282b5cee532dca150c75f5aa5afcaf7bfedcfa3dfb99893e84d7eabe1f36bef7"
PG_ID = "3e8abad67a5a4bbfdb2dd0773287d808e83b49a294634cff9d4d57af8efc5e1d"
POSTMASTER = "2026-09-16 17:37:17.821965+03"
NAMES = [
    "iot_postgres",
    "iot_api",
    "shelly_ingestor",
    "ttn_ingestor",
    "iot_caddy",
    "iot_mosquitto",
]
VOLUME = Path("/mnt/HC_Volume_106884142")
NETWORK = "upat-nzc-mqtt-db_default"


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def out(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(name, value):
    p = RECEIPTS / name
    require(not p.exists(), "Receipt already exists: " + name)
    p.write_text(json.dumps(value, indent=2) + "\n")


def psql(sql):
    return out(
        [
            "docker",
            "exec",
            "iot_postgres",
            "psql",
            "-X",
            "-qAt",
            "-U",
            "postgres",
            "-d",
            "iot_db",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ]
    ).strip()


def inspect(name):
    return json.loads(out(["docker", "inspect", name]))[0]


def snapshot():
    return {
        name: {
            "id": c["Id"],
            "image": c["Image"],
            "started": c["State"]["StartedAt"],
            "restarts": c["RestartCount"],
            "running": c["State"]["Running"],
        }
        for name in NAMES
        for c in [inspect(name)]
    }


def db_guard():
    out(
        [
            "python3",
            "/usr/local/libexec/upat-pg-host-preflight.py",
            "/etc/upat-nzc/postgres-volume.json",
        ]
    )
    require(inspect("iot_postgres")["Id"] == PG_ID, "PostgreSQL identity changed")
    identity = psql(
        "BEGIN READ ONLY; SELECT system_identifier::text||'|'||pg_postmaster_start_time()::text FROM pg_control_system(); COMMIT;"
    )
    require(
        identity == "7618955918777626661|" + POSTMASTER,
        "PostgreSQL cluster/start changed",
    )
    require(
        out(["systemctl", "is-active", "upat-postgres-volume.service"]).strip()
        == "active",
        "PostgreSQL unit inactive",
    )
    health = json.loads(
        out(["curl", "--max-time", "10", "-fsS", "http://127.0.0.1:8000/health"])
    )
    require(health == {"status": "ok", "database": "connected"}, "API health failed")
    if (RECEIPTS / "before.json").exists():
        before = json.loads((RECEIPTS / "before.json").read_text())["containers"]
        now = snapshot()
        for name in NAMES:
            if name != "shelly_ingestor":
                require(
                    now[name] == before[name], "Unrelated container changed: " + name
                )


def compose(path, *args):
    return [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "--env-file",
        str(ROOT / ".env"),
        "-p",
        "upat-nzc-mqtt-db",
        "-f",
        str(path),
        *args,
    ]


def model(path):
    return json.loads(
        out(compose(path, "--profile", "*", "config", "--format", "json"))
    )


def expected_after(before):
    expected = copy.deepcopy(before)
    expected["services"]["shelly-ingestor"]["image"] = IMAGE
    expected["services"]["shelly-ingestor"].setdefault("environment", {})[
        "SHELLY_MEASUREMENTS_WRITE_MODE"
    ] = "dual"
    return expected


def validate_model(value):
    require("postgres" not in value["services"], "Compose must not own PostgreSQL")
    require(
        "postgres_data" not in value.get("volumes", {}),
        "Old PostgreSQL volume declared",
    )
    require(
        value["networks"]["default"]["external"]
        and value["networks"]["default"]["name"] == NETWORK,
        "Wrong network",
    )
    for service in value["services"].values():
        require(
            "postgres" not in service.get("depends_on", {}),
            "PostgreSQL lifecycle dependency",
        )
        require(
            service.get("container_name") != "iot_postgres",
            "PostgreSQL container declared",
        )
        for mount in service.get("volumes", []):
            require(
                mount.get("target") != "/var/lib/postgresql/data",
                "PGDATA declared in Compose",
            )


def verify_artifacts():
    manifest = json.loads((STAGE / "ARTIFACT-MANIFEST.json").read_text())
    for path, digest in manifest["files"].items():
        require(
            not Path(path).is_absolute() and ".." not in Path(path).parts,
            "Invalid artifact path",
        )
        require(sha(STAGE / path) == digest, "Artifact changed: " + path)
    image = inspect(IMAGE)
    require(
        image["Id"] == IMAGE
        and image["Config"]["Labels"]["org.opencontainers.image.revision"]
        == APP_COMMIT,
        "Wrong candidate image",
    )


def atomic_config(data):
    path = ROOT / "docker-compose.prod.yml"
    tmp = ROOT / ".docker-compose.prod.shelly-stage1.tmp"
    require(not tmp.exists() and not tmp.is_symlink(), "Unexpected temporary config")
    mode = path.stat().st_mode & 0o777
    with tmp.open("xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, mode)
    os.replace(tmp, path)
    fd = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def preflight():
    db_guard()
    verify_artifacts()
    require(
        sha(ROOT / "docker-compose.prod.yml") == OLD_COMPOSE, "Canonical Compose drift"
    )
    require(
        (ROOT / "docker-compose.yml").is_symlink()
        and os.readlink(ROOT / "docker-compose.yml") == "docker-compose.prod.yml",
        "Default Compose symlink drift",
    )
    require(
        inspect("shelly_ingestor")["Image"] == OLD_IMAGE, "Original ingestor changed"
    )
    before = model(ROOT / "docker-compose.prod.yml")
    candidate = model(STAGE / "ops/shelly-compact/compose.stage1.prod.yml")
    validate_model(candidate)
    require(
        candidate == expected_after(before),
        "Changes beyond ingestor image and dual flag",
    )
    st = os.statvfs(VOLUME)
    free = st.f_bavail * st.f_frsize
    require(free >= 45 * 1024**3, "Less than initial 45 GiB headroom")
    require(
        psql("SELECT to_regnamespace('shelly_compact') IS NULL") == "t",
        "Compact schema already exists",
    )
    backup = json.loads((STAGE / "BACKUP-GATE.json").read_text())
    require(
        backup["restore"] == "PASS" and backup["chris_copy"] == "PASS",
        "Backup gate incomplete",
    )
    (RECEIPTS / "compose.before.yml").write_bytes(
        (ROOT / "docker-compose.prod.yml").read_bytes()
    )
    save(
        "before.json",
        {
            "time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "containers": snapshot(),
            "env_sha": sha(ROOT / ".env"),
            "volume_free_bytes": free,
            "backup": backup,
        },
    )
    return {
        "status": "PASS",
        "only_changes": [
            "shelly-ingestor.image",
            "shelly-ingestor.environment.SHELLY_MEASUREMENTS_WRITE_MODE",
        ],
        "volume_free_bytes": free,
    }


def run_migration(phase, args=()):
    env_values = dict(
        v.split("=", 1) for v in inspect("shelly_ingestor")["Config"]["Env"]
    )
    values = {
        "host": env_values["POSTGRES_HOST"],
        "port": env_values["POSTGRES_INTERNAL_PORT"],
        "dbname": env_values["POSTGRES_DB"],
        "user": env_values["POSTGRES_USER"],
        "password": env_values["POSTGRES_PASSWORD"],
    }
    require(
        values["host"] == "postgres"
        and values["dbname"] == "iot_db"
        and values["user"] == "postgres",
        "Unexpected database destination",
    )
    quote = lambda value: "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    env = os.environ.copy()
    env["SHELLY_MIGRATION_DSN"] = " ".join(
        k + "=" + quote(v) for k, v in values.items()
    )
    env["PGOPTIONS"] = (
        "-c work_mem=32MB -c maintenance_work_mem=128MB -c temp_file_limit=12GB -c max_parallel_workers_per_gather=0"
    )
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        "shelly_compact_migration_job",
        "--network",
        NETWORK,
        "--cpus",
        "0.75",
        "--memory",
        "512m",
        "--read-only",
        "--mount",
        f"type=bind,src={STAGE},dst=/stage,readonly",
        "--mount",
        f"type=bind,src={RECEIPTS},dst=/receipts",
        "--mount",
        f"type=bind,src={VOLUME},dst=/space-check,readonly",
        "-e",
        "SHELLY_MIGRATION_DSN",
        "-e",
        "PGOPTIONS",
        "--entrypoint",
        "python",
        IMAGE,
    ]
    if phase == "copy-window":
        command += ["/stage/ops/shelly-compact/copy-window.py", *args]
    else:
        command += [
            "/release/ops/shelly-compact/migrate.py",
            phase,
            "--production",
            "--expected-system-id",
            "7618955918777626661",
            "--space-check-path",
            "/space-check",
            *args,
        ]
    subprocess.run(command, env=env, check=True)


def activate():
    db_guard()
    verify_artifacts()
    before = json.loads((RECEIPTS / "before.json").read_text())
    require(
        sha(ROOT / "docker-compose.prod.yml") == OLD_COMPOSE
        and sha(ROOT / ".env") == before["env_sha"],
        "Config changed since preflight",
    )
    require(
        snapshot()["shelly_ingestor"] == before["containers"]["shelly_ingestor"],
        "Ingestor changed since preflight",
    )
    started = time.time()
    recreated = None
    try:
        subprocess.run(
            ["docker", "stop", "--timeout", "30", "shelly_ingestor"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        run_migration(
            "capture", ["--writers-paused", "--receipt", "/receipts/checkpoint.json"]
        )
        atomic_config(
            (STAGE / "ops/shelly-compact/compose.stage1.prod.yml").read_bytes()
        )
        subprocess.run(
            compose(
                ROOT / "docker-compose.prod.yml",
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--pull",
                "never",
                "--force-recreate",
                "shelly-ingestor",
            ),
            check=True,
        )
        recreated = time.time()
        high = json.loads((RECEIPTS / "checkpoint.json").read_text())["high_water"]
        for _ in range(40):
            current = inspect("shelly_ingestor")
            require(
                current["State"]["Running"] and current["RestartCount"] == 0,
                "Candidate unhealthy",
            )
            require(current["Image"] == IMAGE, "Wrong running candidate")
            require(
                "SHELLY_MEASUREMENTS_WRITE_MODE=dual" in current["Config"]["Env"],
                "Dual mode absent",
            )
            n = int(
                psql(
                    f"SELECT count(*) FROM shelly_compact.measurements WHERE id>{high}"
                )
            )
            if n > 0:
                break
            time.sleep(1)
        require(n > 0, "No new dual measurements observed")
        db_guard()
        require(
            model(ROOT / "docker-compose.prod.yml")
            == expected_after(model(RECEIPTS / "compose.before.yml")),
            "Installed model drift",
        )
        require(sha(ROOT / ".env") == before["env_sha"], "Environment file changed")
        result = {
            "status": "PASS",
            "source_commit": APP_COMMIT,
            "image": IMAGE,
            "candidate": snapshot()["shelly_ingestor"],
            "checkpoint": high,
            "new_compact_rows_observed": n,
            "stop_to_recreate_seconds": round(recreated - started, 2),
            "postgres_and_api_unchanged": True,
            "mode": "dual",
            "time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        save("activated.json", result)
        return result
    except BaseException:
        atomic_config((RECEIPTS / "compose.before.yml").read_bytes())
        subprocess.run(
            compose(
                ROOT / "docker-compose.prod.yml",
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--pull",
                "never",
                "--force-recreate",
                "shelly-ingestor",
            ),
            check=True,
        )
        db_guard()
        save(
            "activation-rollback.json",
            {
                "original_ingestor_restored": inspect("shelly_ingestor")["Image"]
                == OLD_IMAGE,
                "postgres_and_api_unchanged": True,
            },
        )
        raise


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser()
    p.add_argument(
        "phase",
        choices=[
            "preflight",
            "prepare",
            "activate",
            "copy-window",
            "index",
            "verify",
            "status",
        ],
    )
    p.add_argument("extra", nargs=argparse.REMAINDER)
    a = p.parse_args()
    require(os.geteuid() == 0, "Production runner requires root")
    RECEIPTS.mkdir(mode=0o700, exist_ok=True)
    with open("/run/lock/shelly-compact-stage1.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if a.phase == "preflight":
            result = preflight()
        elif a.phase == "activate":
            result = activate()
        else:
            db_guard()
            verify_artifacts()
            require((RECEIPTS / "before.json").exists(), "Missing preflight receipt")
            if a.phase == "status":
                result = {"status": "PASS", "containers": snapshot()}
            else:
                if a.phase != "prepare":
                    require(
                        (RECEIPTS / "activated.json").exists(),
                        "Dual activation not verified",
                    )
                run_migration(a.phase, a.extra)
                db_guard()
                result = {
                    "status": "PASS",
                    "phase": a.phase,
                    "postgres_and_api_unchanged": True,
                }
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
