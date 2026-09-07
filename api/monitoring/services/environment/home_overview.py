"""Home-only aggregation with bounded, per-process TTL caches and single-flight loads.
Authorization and catalog membership are checked by the route before cache access.
"""
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from time import monotonic
from typing import Callable

from fastapi import HTTPException
from monitoring.schemas import DeviceHistoryResponse
from monitoring.services.service_overview import get_device_history, get_device_rolling_twenty_four_hour_hourly_series


@dataclass(frozen=True)
class CachedHistory:
    data: DeviceHistoryResponse
    fetched_at: datetime
    expires: float
    stale: bool = False


class HomeCache:
    def __init__(self, clock=monotonic, capacity=512):
        self.clock = clock
        self.capacity = capacity
        self.entries = OrderedDict()
        self.inflight: dict[tuple, Future] = {}
        self.lock = Lock()

    def get(self, key: tuple, loader: Callable, refresh=False) -> CachedHistory:
        with self.lock:
            previous = self.entries.get(key)
            if previous and previous.expires > self.clock() and not refresh:
                self.entries.move_to_end(key)
                return previous
            future = self.inflight.get(key)
            owner = future is None
            if owner:
                future = Future()
                self.inflight[key] = future
        if not owner:
            return future.result()
        try:
            data = loader()
            # Reject wrong identities, nonfinite metrics and malformed counts
            # before sharing the result across authorized requests.
            if data.device_id != key[1] or data.count != len(data.items) or any(i.device_id != key[1] for i in data.items):
                raise HTTPException(502, 'Invalid device history')
            import math
            if any(not math.isfinite(r.value) for i in data.items for r in i.measurements.values()):
                raise HTTPException(502, 'Invalid device measurement')
            ttl = 60 if key[2] == 'live' else 300
            result = CachedHistory(data, datetime.now(timezone.utc), self.clock() + ttl)
            with self.lock:
                self.entries[key] = result
                self.entries.move_to_end(key)
                while len(self.entries) > self.capacity:
                    self.entries.popitem(last=False)
            future.set_result(result)
            return result
        except Exception as exc:
            # Expired data remains explicitly marked stale, with its original age.
            if previous and self.clock() - previous.expires < 900:
                result = CachedHistory(previous.data, previous.fetched_at, previous.expires, True)
                future.set_result(result)
                return result
            future.set_exception(exc)
            raise
        finally:
            with self.lock:
                self.inflight.pop(key, None)

    def clear(self):
        """Maintenance/test hook; not exposed over HTTP."""
        with self.lock:
            self.entries.clear()

    def expire_live(self):
        """Timing-audit hook: expire only the live partition without waiting."""
        with self.lock:
            for key, entry in list(self.entries.items()):
                if key[2] == 'live':
                    self.entries[key] = CachedHistory(entry.data, entry.fetched_at, self.clock()-1)


home_cache = HomeCache()
# Shared worker cap across requests, rather than one unbounded pool per user.
home_workers = ThreadPoolExecutor(max_workers=8, thread_name_prefix='home-history')


def resolve_home_rooms(room_ids: list[str], devices: list[dict]):
    room_devices = {room: [d for d in devices if d['room_id'] == room] for room in room_ids}
    if any(not value for value in room_devices.values()):
        raise HTTPException(404, 'One or more rooms are not available in this school.')
    if sum(map(len, room_devices.values())) > 24:
        raise HTTPException(422, 'Request fewer rooms.')
    return room_devices


def iter_home_overview(school_id: str, room_devices: dict, refresh=False):
    """Yield independent live/history parts as ready, with no polling or refetch.

    A slow room never holds up another room's live value. Multi-sensor rooms
    still wait for their own sensors before choosing a coherent snapshot.
    """
    def load(device, kind):
        loader = (lambda: get_device_history(device, aggregate='avg', interval="1m", limit=1)) if kind == 'live' else (lambda: get_device_rolling_twenty_four_hour_hourly_series(device))
        return home_cache.get((school_id, device, kind), loader, refresh)

    pending = {}
    candidates = {room: [] for room in room_devices}
    errors = {room: [] for room in room_devices}
    live_remaining = {room: len(ds) for room, ds in room_devices.items()}
    # Submit selected-room work before neighboring rooms. The pool cap remains
    # shared across requests; adding streaming doesn't add upstream calls.
    for room, ds in room_devices.items():
        for device in ds:
            pending[home_workers.submit(load, device['id'], 'live')] = (room, device['id'], 'live')
        if len(ds) == 1:
            pending[home_workers.submit(load, ds[0]['id'], 'history')] = (room, ds[0]['id'], 'history')
    try:
        while pending:
            ready, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in ready:
                room, device_id, kind = pending.pop(future)
                try:
                    entry = future.result()
                    failure = None
                except Exception:
                    entry = None
                    failure = kind + '_unavailable'
                if kind == 'history':
                    yield dict(school_id=school_id, rooms=[dict(
                        room_id=room, device_id=device_id, part='history', history=entry.data if entry else None,
                        history_fetched_at=entry.fetched_at if entry else None,
                        errors=[failure] if failure else (['history_stale'] if entry.stale else []))])
                    continue
                if failure: errors[room].append(failure)
                if entry:
                    if entry.stale: errors[room].append('live_stale')
                    for item in entry.data.items:
                        if item.measurements:
                            candidates[room].append((item.event_time, device_id, entry, item))
                live_remaining[room] -= 1
                if live_remaining[room]: continue
                # Equal timestamps are common with minute buckets. A stable
                # device tie-break avoids changing sensors with completion
                # order and needlessly loading a different sensor's history.
                chosen = max(candidates[room], key=lambda row: (row[0], row[1])) if candidates[room] else None
                device_id = chosen[1] if chosen else room_devices[room][0]['id']
                live = chosen[2] if chosen else None
                live_data = DeviceHistoryResponse(device_id=device_id, count=1, items=[chosen[3]]) if chosen else None
                yield dict(school_id=school_id, rooms=[dict(
                    room_id=room, device_id=device_id, part='live', live=live_data,
                    live_fetched_at=live.fetched_at if live else None, errors=sorted(set(errors[room])))])
                if len(room_devices[room]) > 1:
                    pending[home_workers.submit(load, device_id, 'history')] = (room, device_id, 'history')
    finally:
        # A disconnected client must not leave queued work behind. Already
        # running upstream requests can finish and populate the shared cache.
        for future in pending: future.cancel()


def get_home_overview(school_id: str, room_ids: list[str], devices: list[dict], refresh=False):
    room_devices = resolve_home_rooms(room_ids, devices)
    merged = {}
    for update in iter_home_overview(school_id, room_devices, refresh):
        for part in update['rooms']:
            room = merged.setdefault(part['room_id'], dict(room_id=part['room_id'], device_id=part['device_id'],
                live=None, history=None, live_fetched_at=None, history_fetched_at=None, errors=[]))
            kind = part['part']
            room[kind] = part.get(kind)
            room[kind + '_fetched_at'] = part.get(kind + '_fetched_at')
            room['errors'] = sorted(set(room['errors'] + part['errors']))
    return dict(school_id=school_id, rooms=[merged[room] for room in room_ids])
