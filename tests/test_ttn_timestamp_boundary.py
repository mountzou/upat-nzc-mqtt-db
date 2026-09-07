"""Exercise the ingestion validator without starting the MQTT transport."""
import ast
import re
from datetime import datetime
from pathlib import Path

import pytest

source = Path(__file__).resolve().parents[1] / "ttn-ingestor/main.py"
tree = ast.parse(source.read_text())
validator = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == "validated_event_time")
namespace = {"datetime": datetime, "re": re}
exec(compile(ast.Module(body=[validator], type_ignores=[]), str(source), "exec"), namespace)
validate = namespace["validated_event_time"]


@pytest.mark.parametrize("value", [
    "2026-09-06T12:00:00.123456789Z", "2026-09-06T15:00:00+03:00",
    "2026-10-25T03:30:00+03:00", "2026-10-25T03:30:00+02:00",
])
def test_source_offset_and_subsecond_precision_are_preserved(value):
    assert validate(value) == value


@pytest.mark.parametrize("value", [None, "", "2026-09-06T15:00:00", "invalid"])
def test_missing_or_ambiguous_source_time_is_rejected(value):
    with pytest.raises(ValueError):
        validate(value)
