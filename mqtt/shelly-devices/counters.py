"""Normalize cumulative Shelly counters; timestamps belong to the receiver.

EMData reports imported and returned Wh separately. Switch aenergy includes
both directions; retain both counters so the aggregator can derive imports
and detect a reset of either counter. Never use by_minute (mWh) as Wh.
"""
import math


def valid_counter(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def counter_rows(device_id, topic, payload, received_at):
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("Counter timestamps must be aware")
    if not isinstance(payload, dict) or payload.get('errors'):
        return []
    component = topic.removeprefix(device_id + '/status/')
    rows = []
    if device_id.startswith('shellypro3em') and component == 'emdata:0':
        for phase in 'abc':
            value = payload.get(f'{phase}_total_act_energy')
            returned = payload.get(f'{phase}_total_act_ret_energy')
            if valid_counter(value) and (returned is None or valid_counter(returned)):
                rows.append((device_id, phase, received_at, float(value),
                             float(returned) if returned is not None else None,
                             'import', component))
    elif device_id.startswith('shellyplug') and component == 'switch:0':
        energy = payload.get('aenergy')
        returned_section = payload.get('ret_aenergy')
        if not isinstance(energy, dict):
            return []
        value = energy.get('total')
        if returned_section is not None and not isinstance(returned_section, dict):
            return []
        returned = returned_section.get('total') if returned_section is not None else None
        if returned_section is not None and not valid_counter(returned):
            return []
        if valid_counter(value) and (returned is None or returned <= value):
            rows.append((device_id, 'total', received_at, float(value),
                         float(returned) if returned is not None else None,
                         'absolute', component))
    return rows


def insert_counters(conn, device_id, topic, payload, received_at):
    rows = counter_rows(device_id, topic, payload, received_at)
    if rows:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO shelly_energy_counters
                  (device_id, channel, observed_at, energy_wh, returned_energy_wh,
                   counter_kind, source_component)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, rows)
