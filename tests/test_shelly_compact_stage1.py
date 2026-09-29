"""Production stage-one scope and failure recovery without production access."""

import importlib.util
import json
from pathlib import Path
import subprocess

import pytest
from test_postgres_volume_compose import model

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "stage1", ROOT / "ops/shelly-compact/stage1.py"
)
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


def test_resolved_stage1_changes_only_writer_image_and_dual_flag(tmp_path):
    baseline = tmp_path / "before.yml"
    baseline.write_bytes(
        subprocess.check_output(
            ["git", "show", "c7463cb:docker-compose.prod.yml"], cwd=ROOT
        )
    )
    before = model(baseline)
    candidate = model(ROOT / "ops/shelly-compact/compose.stage1.prod.yml")
    assert candidate == s.expected_after(before)
    s.validate_model(candidate)
    assert candidate["services"]["api"] == before["services"]["api"]
    assert candidate["services"]["ttn-ingestor"] == before["services"]["ttn-ingestor"]


@pytest.mark.parametrize(
    "mutation", ["service", "volume", "dependency", "mount", "network"]
)
def test_database_lifecycle_drift_is_rejected(mutation):
    candidate = {
        "services": {"shelly-ingestor": {}},
        "networks": {"default": {"external": True, "name": s.NETWORK}},
    }
    if mutation == "service":
        candidate["services"]["postgres"] = {}
    if mutation == "volume":
        candidate["volumes"] = {"postgres_data": {}}
    if mutation == "dependency":
        candidate["services"]["shelly-ingestor"]["depends_on"] = {"postgres": {}}
    if mutation == "mount":
        candidate["services"]["shelly-ingestor"]["volumes"] = [
            {"target": "/var/lib/postgresql/data"}
        ]
    if mutation == "network":
        candidate["networks"]["default"]["external"] = False
    with pytest.raises(RuntimeError):
        s.validate_model(candidate)


def test_failed_checkpoint_restores_only_original_writer(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    cfg = root / "docker-compose.prod.yml"
    cfg.write_text("original fixture compose")
    (root / ".env").write_text("POSTGRES_DB=fixture")
    (receipts / "compose.before.yml").write_bytes(cfg.read_bytes())
    baseline = {"shelly_ingestor": {"id": "original"}}
    (receipts / "before.json").write_text(
        json.dumps({"env_sha": s.sha(root / ".env"), "containers": baseline})
    )
    monkeypatch.setattr(s, "ROOT", root)
    monkeypatch.setattr(s, "RECEIPTS", receipts)
    monkeypatch.setattr(s, "OLD_COMPOSE", s.sha(cfg))
    monkeypatch.setattr(s, "db_guard", lambda: None)
    monkeypatch.setattr(s, "verify_artifacts", lambda: None)
    monkeypatch.setattr(s, "snapshot", lambda: baseline)
    monkeypatch.setattr(s, "inspect", lambda _: {"Image": s.OLD_IMAGE})
    commands = []
    monkeypatch.setattr(
        s.subprocess, "run", lambda args, **kwargs: commands.append(args)
    )

    def fail(*args):
        raise RuntimeError("checkpoint rejected")

    monkeypatch.setattr(s, "run_migration", fail)
    with pytest.raises(RuntimeError, match="checkpoint rejected"):
        s.activate()
    assert cfg.read_text() == "original fixture compose"
    assert commands[0] == ["docker", "stop", "--timeout", "30", "shelly_ingestor"]
    assert len(commands) == 2
    assert (
        commands[1][-1] == "shelly-ingestor"
        and "--no-deps" in commands[1]
        and "--no-build" in commands[1]
    )
    assert not any(
        "iot_postgres" in arg or arg == "restart" or arg == "down"
        for cmd in commands
        for arg in cmd
    )
    assert (
        json.loads((receipts / "activation-rollback.json").read_text())[
            "original_ingestor_restored"
        ]
        is True
    )
