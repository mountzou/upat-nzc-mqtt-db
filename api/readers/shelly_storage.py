"""Storage selection without changing HTTP response fields or raw values."""

import os

READ_STORAGE = os.getenv("SHELLY_MEASUREMENTS_READ_STORAGE", "legacy")
ROUNDING = os.getenv("SHELLY_MEASUREMENTS_ROUNDING", "legacy")
if READ_STORAGE not in {"legacy", "compact"}:
    raise ValueError("Invalid SHELLY_MEASUREMENTS_READ_STORAGE")
if ROUNDING not in {"legacy", "decimal_1"}:
    raise ValueError("Invalid SHELLY_MEASUREMENTS_ROUNDING")
if READ_STORAGE == "compact" and ROUNDING != "decimal_1":
    raise ValueError(
        "Compact Shelly reads require the verified decimal_1 rounding policy"
    )


def source_and_average(table):
    if table not in {"shelly_measurements", "upat_measurements"}:
        raise ValueError("Unknown measurement source")
    if table == "shelly_measurements":
        relation = (
            "shelly_compact.readings"
            if READ_STORAGE == "compact"
            else "shelly_measurements"
        )
        average = (
            "ROUND(AVG(value::numeric), 1)::double precision"
            if ROUNDING == "decimal_1"
            else "AVG(value)"
        )
        return relation, average
    return table, "AVG(value)"
