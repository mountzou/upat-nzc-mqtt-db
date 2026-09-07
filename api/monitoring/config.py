import os
from pathlib import Path

APP_TIMEZONE_NAME = "Europe/Athens"
DIR_ROOMS = Path(__file__).parent / "catalog"
DEVICE_API_BASE_URL = ""

ENERGY_CO2_FACTOR_KG_PER_KWH = max(
    0.0,
    float(os.getenv("ENERGY_CO2_FACTOR_KG_PER_KWH", "0.285")),
)
ENERGY_CO2_FACTOR_SOURCE = os.getenv(
    "ENERGY_CO2_FACTOR_SOURCE",
    "SchoolHeroz project grid factor",
).strip()
_energy_co2_reference_year = os.getenv(
    "ENERGY_CO2_FACTOR_REFERENCE_YEAR",
    "",
).strip()
ENERGY_CO2_FACTOR_REFERENCE_YEAR = (
    int(_energy_co2_reference_year) if _energy_co2_reference_year else None
)
ENERGY_CO2_FACTOR_VERSION = os.getenv(
    "ENERGY_CO2_FACTOR_VERSION",
    "project-grid-factor-v1",
).strip()
