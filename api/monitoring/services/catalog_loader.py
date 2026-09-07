"""Shared loading and validation for local JSON catalogs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypeVar

from pydantic import TypeAdapter

CatalogT = TypeVar("CatalogT")


def load_validated_json_catalog(
    path: Path,
    adapter: TypeAdapter[CatalogT],
    *,
    catalog_name: str = "catalog",
) -> CatalogT:
    """Read and validate one catalog without imposing a cache policy."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"{catalog_name.capitalize()} file not found: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {catalog_name} file {path}: {exc}"
        ) from exc

    return adapter.validate_python(payload)
