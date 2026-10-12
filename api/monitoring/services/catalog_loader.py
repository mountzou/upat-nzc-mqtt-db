"""Shared loading and validation for the local device catalog."""

from __future__ import annotations

import json

from pydantic import TypeAdapter

from monitoring.config import DIR_ROOMS
from monitoring.schemas import SchoolDeviceCatalogEntry

_DEVICE_CATALOG_ADAPTER = TypeAdapter(list[SchoolDeviceCatalogEntry])


def load_device_catalog() -> list[SchoolDeviceCatalogEntry]:
    """Read and validate both device groups without imposing a cache policy."""
    path = DIR_ROOMS / "device_assignments.json"
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Device catalog file not found: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in device catalog file {path}: {exc}"
        ) from exc

    return _DEVICE_CATALOG_ADAPTER.validate_python(payload)
