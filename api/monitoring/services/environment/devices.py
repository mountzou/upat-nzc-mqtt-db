from pydantic import TypeAdapter

from monitoring.config import DIR_ROOMS
from monitoring.schemas import SchoolEnvironmentDeviceCatalogEntry
from monitoring.services.catalog_loader import load_validated_json_catalog

_ENVIRONMENT_DEVICE_CATALOG_ADAPTER = TypeAdapter(list[SchoolEnvironmentDeviceCatalogEntry])


def _load_environment_device_catalog():
    return load_validated_json_catalog(
        DIR_ROOMS / "device_assignments.json",
        _ENVIRONMENT_DEVICE_CATALOG_ADAPTER,
        catalog_name="environment device catalog",
    )


def list_devices_for_school(school_id: str):
    for school_entry in _load_environment_device_catalog():
        if school_entry.school_id == school_id:
            return [device.model_dump() for device in school_entry.devices]

    return []


def find_school_id_for_device(device_id: str) -> str | None:
    for school_entry in _load_environment_device_catalog():
        if any(device.id == device_id for device in school_entry.devices):
            return school_entry.school_id
    return None
