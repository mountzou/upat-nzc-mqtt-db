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
            result = main.get_shelly_hourly_energy(
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
            elif family=='shelly' and action=='energy':
                result=phase_energy_estimate(device, values['start'], values['end'], values.get('bucket_minutes',30))
            else:
                raise ValueError('Unsupported local monitoring query')
        return jsonable_encoder(result)
    finally:
        _reads.release()


def phase_energy_estimate(device_id, start, end, bucket_minutes):
    """Bounded legacy phase estimate, integrated in SQL with clipped end buckets."""
    import main
    from datetime import datetime, timedelta
    first, stop = datetime.fromisoformat(start), datetime.fromisoformat(end)
    if not 1 <= bucket_minutes <= 10080 or not timedelta(0) < stop-first <= timedelta(days=90):
        raise HTTPException(400, 'Invalid phase-energy range or bucket size')
    spec = main.get_shelly_device_db_table(device_id)
    if spec['device_type'] != 'pro3em':
        raise HTTPException(400, 'Phase energy requires a Pro 3EM device')
    try:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '15s'")
                cur.execute("""
                    WITH buckets AS (
                        SELECT metric, date_bin(%s::interval, event_time, %s::timestamp) AS bucket,
                               AVG(value) AS mean_w
                        FROM shelly_measurements
                        WHERE device_id = %s AND event_time >= %s AND event_time < %s
                          AND metric IN ('a_act_power','b_act_power','c_act_power')
                          AND value IS NOT NULL
                        GROUP BY metric, bucket
                    )
                    SELECT metric, SUM(mean_w * EXTRACT(EPOCH FROM
                        (LEAST(bucket + %s::interval, %s::timestamp) - bucket)) / 3600.0) AS wh
                    FROM buckets GROUP BY metric
                """, (timedelta(minutes=bucket_minutes),first,device_id,first,stop,
                      timedelta(minutes=bucket_minutes),stop))
                rows=cur.fetchall()
    except Exception as exc:
        raise HTTPException(503, 'Phase energy is temporarily unavailable') from exc
    import math
    values={row['metric'][0]:float(row['wh']) for row in rows}
    if set(values) != {'a','b','c'}:
        raise HTTPException(404, 'No complete phase-energy observation for this period')
    if any(not math.isfinite(v) for v in values.values()):
        raise HTTPException(502, 'Invalid phase-energy source data')
    return {'device_id':device_id,'start':first,'end':stop,'bucket_minutes':bucket_minutes,
            'energy_wh':{**values,'total':math.fsum(values.values())}}
