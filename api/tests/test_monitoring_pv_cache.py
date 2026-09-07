from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from threading import Event
import pytest
from monitoring.services.energy_production import energy as pv

DAY=date(2026,9,1)

def prepared(start,end,*args):
    return {d:[] for d in pv.date_range(start,end)}

def test_identical_inflight_reads_share_query(monkeypatch):
    cache=pv.EnergyDayCache(); entered=Event(); release=Event(); calls=[]
    def query(*args):
        calls.append(args); entered.set(); assert release.wait(3); return prepared(*args)
    monkeypatch.setattr(pv,'query_days',query)
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(cache.get,DAY,DAY,pv.parse_interval('1h'),'history'); assert entered.wait(3)
        second=pool.submit(cache.get,DAY,DAY,pv.parse_interval('1h'),'history');release.set()
        assert first.result()==second.result()
    assert len(calls)==1

def test_failure_not_cached_and_ttl_and_capacity_are_bounded(monkeypatch):
    now=[0];cache=pv.EnergyDayCache(capacity=2,ttl=5,clock=lambda:now[0]);calls=[]
    def query(*args):
        calls.append(args)
        if len(calls)==1: raise RuntimeError('fixture unavailable')
        return prepared(*args)
    monkeypatch.setattr(pv,'query_days',query)
    with pytest.raises(RuntimeError):cache.get(DAY,DAY,pv.parse_interval('1h'),'history')
    assert not cache.inflight and not cache.entries
    cache.get(DAY,DAY,pv.parse_interval('1h'),'history');cache.get(DAY,DAY,pv.parse_interval('1h'),'history');assert len(calls)==2
    now[0]=6;cache.get(DAY,DAY,pv.parse_interval('1h'),'history');assert len(calls)==3
    cache.get(DAY+timedelta(days=1),DAY+timedelta(days=3),pv.parse_interval('1h'),'history');assert len(cache.entries)==2
