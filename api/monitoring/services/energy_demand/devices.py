from monitoring.schemas import EnergyDeviceAssignments
from monitoring.services.catalog_loader import load_device_catalog

WHOLE_BUILDING_ROOM_ID = "all_rooms"


def get_school_energy_catalog(school_id: str) -> EnergyDeviceAssignments | None:
    for school_entry in load_device_catalog():
        if school_entry.school_id == school_id:
            return school_entry.energy

    return None


def list_energy_devices_for_school(school_id: str):
    school_entry = get_school_energy_catalog(school_id)
    return [device.model_dump() for device in school_entry.devices] if school_entry else []


def find_school_id_for_energy_device(device_id: str) -> str | None:
    for school_entry in load_device_catalog():
        if any(device.id == device_id for device in school_entry.energy.devices):
            return school_entry.school_id
    return None


def is_whole_building_energy_meter(device: dict) -> bool:
    """Return whether a catalog device is the authoritative school-total meter."""
    return (
        device.get("type") == "three_phase_meter"
        and str(device.get("room_id") or "").strip() == WHOLE_BUILDING_ROOM_ID
    )
