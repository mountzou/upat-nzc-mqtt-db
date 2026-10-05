"""Compact Shelly storage and stable averages for the shared readers."""


def source_and_average(table):
    if table == "shelly_measurements":
        return "shelly_compact.readings", "ROUND(AVG(value::numeric), 1)::double precision"
    if table == "upat_measurements":
        return table, "AVG(value)"
    raise ValueError("Unknown measurement source")
