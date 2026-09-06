from pydantic import TypeAdapter

from monitoring.config import DIR_ROOMS
from monitoring.schemas import SchoolEnergyDeviceCatalogEntry
from monitoring.services.catalog_loader import load_validated_json_catalog

_ENERGY_DEVICE_CATALOG_ADAPTER = TypeAdapter(list[SchoolEnergyDeviceCatalogEntry])

WHOLE_BUILDING_ROOM_ID = "all_rooms"


def _load_energy_device_catalog():
    return load_validated_json_catalog(
        DIR_ROOMS / "energy_device_assignments.json",
        _ENERGY_DEVICE_CATALOG_ADAPTER,
        catalog_name="energy device catalog",
    )


def list_energy_devices_for_school(school_id: str):
    for school_entry in _load_energy_device_catalog():
        if school_entry.school_id == school_id:
            return [device.model_dump() for device in school_entry.devices]

    return []


def find_school_id_for_energy_device(device_id: str) -> str | None:
    for school_entry in _load_energy_device_catalog():
        if any(device.id == device_id for device in school_entry.devices):
            return school_entry.school_id
    return None


def is_whole_building_energy_meter(device: dict) -> bool:
    """Return whether a catalog device is the authoritative school-total meter."""
    return (
        device.get("type") == "three_phase_meter"
        and str(device.get("room_id") or "").strip() == WHOLE_BUILDING_ROOM_ID
    )
