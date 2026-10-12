"""Shared local device-history reads from validated service parameters."""
from typing import Literal

from fastapi.encoders import jsonable_encoder
from schemas import HistoryQueryParams
from monitoring.read_limits import monitoring_read


def read_device_history(
    family: Literal["upat", "shelly"],
    device_id: str,
    query: HistoryQueryParams,
):
    import main

    with monitoring_read():
        if family == "upat":
            result = main.fetch_upat_device_history(device_id, query)
        elif family == "shelly":
            result = main.fetch_device_history("shelly_measurements", device_id, query)
        else:
            raise ValueError("Unknown device family")
        return jsonable_encoder(result)
