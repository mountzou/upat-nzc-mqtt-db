from monitoring.services.catalog_loader import load_device_catalog


def list_devices_for_school(school_id: str):
    for school_entry in load_device_catalog():
        if school_entry.school_id == school_id:
            return [device.model_dump() for device in school_entry.environment.devices]

    return []


def find_school_id_for_device(device_id: str) -> str | None:
    for school_entry in load_device_catalog():
        if any(device.id == device_id for device in school_entry.environment.devices):
            return school_entry.school_id
    return None
