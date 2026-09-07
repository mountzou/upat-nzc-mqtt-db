"""Call the canonical VPS queries directly; monitoring has no HTTP dependency."""
from threading import BoundedSemaphore
from urllib.parse import urlsplit
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from schemas import HistoryQueryParams

_reads = BoundedSemaphore(8)

def local_read(url, params=None):
    import main
    values = dict(params or {})
    if isinstance(params, (list, tuple)):
        values['device_id'] = [v for k,v in params if k=='device_id']
    path = urlsplit(url).path
    if not _reads.acquire(timeout=5):
        raise HTTPException(503, "Monitoring data is busy. Try again shortly.")
    try:
        if path == '/shelly/hourly-energy':
            result = main.fetch_shelly_hourly_energy_rows(
                device_id=values.get('device_id'), start=values.get('start'),
                end=values.get('end'), working_only=values.get('working_only', False))
        else:
            parts=path.strip('/').split('/')
            if len(parts)!=4 or parts[0] not in {'upat','shelly'} or parts[1]!='device':
                raise ValueError('Unsupported local monitoring query')
            family, _, device, action=parts
            table='upat_measurements' if family=='upat' else 'shelly_measurements'
            if action=='latest':
                result=main.fetch_device_latest(table,device,values.get('metric'),values.get('limit',1))
            elif action=='history':
                query=HistoryQueryParams.model_validate(values)
                result=(main.fetch_upat_device_history(device,query) if family=='upat'
                        else main.fetch_device_history(table,device,query))
            else:
                raise ValueError('Unsupported local monitoring query')
        return jsonable_encoder(result)
    finally:
        _reads.release()
