"""Remaining device-history adapter; latest and hourly energy use direct readers."""
from urllib.parse import urlsplit

from fastapi.encoders import jsonable_encoder
from schemas import HistoryQueryParams
from monitoring.read_limits import monitoring_read


def local_read(url, params=None):
    import main

    values = dict(params or {})
    path = urlsplit(url).path
    with monitoring_read():
        parts = path.strip('/').split('/')
        if len(parts) != 4 or parts[0] not in {'upat', 'shelly'} or parts[1] != 'device':
            raise ValueError('Unsupported local monitoring query')
        family, _, device, action = parts
        table = 'upat_measurements' if family == 'upat' else 'shelly_measurements'
        if action != 'history':
            raise ValueError('Unsupported local monitoring query')
        query = HistoryQueryParams.model_validate(values)
        result = (main.fetch_upat_device_history(device, query) if family == 'upat'
                  else main.fetch_device_history(table, device, query))
        return jsonable_encoder(result)
