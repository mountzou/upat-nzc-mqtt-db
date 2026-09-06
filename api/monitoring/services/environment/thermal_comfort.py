"""Calculations for thermal comfort metrics using `pythermalcomfort`."""

from pythermalcomfort.models import discomfort_index
from pythermalcomfort.models import humidex

_DISCOMFORT_CONDITION_LABELS = {
    "More than 50% feels discomfort": ">50% feels discomfort",
}


def _format_discomfort_condition(condition: str) -> str:
    return _DISCOMFORT_CONDITION_LABELS.get(condition, condition)


def _scalar(value):
    return value[0] if isinstance(value, list) else value


# Calculates Discomfort Index (DI) and the corresponding discomfort condition
def calc_discomfort_index(tdb: float, rh: float) -> tuple[float, str]:
    result = discomfort_index(tdb=tdb, rh=rh)
    return float(_scalar(result.di)), _format_discomfort_condition(str(_scalar(result.discomfort_condition)))


# Calculates Humidex and the corresponding discomfort condition
def calc_hum_index(tdb: float, rh: float) -> tuple[float, str]:
    result = humidex(tdb=tdb, rh=rh)
    return float(_scalar(result.humidex)), str(_scalar(result.discomfort))
