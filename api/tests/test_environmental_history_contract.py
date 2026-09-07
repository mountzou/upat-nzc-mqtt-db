from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
import main
from monitoring.routes.auth import get_current_user
from monitoring.routes import indoor_environment as route
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.environment.history.contract import resolve_history_plan

NOW = datetime(2026,9,7,10,17,tzinfo=timezone.utc)
BASE = '/indoor_environment/devices/portable-108/history'

def test_window_and_clock_have_distinct_boundaries():
    a=resolve_history_plan(window='1h',interval='1h',alignment='window',now=NOW)
    b=resolve_history_plan(window='1h',interval='1h',alignment='clock',now=NOW)
    assert (a.start.hour,a.start.minute,a.end.hour,a.end.minute)==(9,17,10,17)
    assert (b.start.hour,b.start.minute,b.end.hour,b.end.minute)==(9,0,10,0)
    assert len(a.buckets)==len(b.buckets)==1

@pytest.mark.parametrize('kwargs',[
    {'start':NOW}, {'start':NOW,'end':NOW},
    {'start':NOW,'end':NOW,'window':'1h'},
    {'window':'1h','alignment':'other'},
    {'window':'1h','alignment':'clock','interval':'5m'},
    {'window':'24h','alignment':'window','interval':'day'},
    {'window':'1h','limit':0}, {'window':'24h','limit':1},
    {'window':'9000h'}, {'metric':['']}, {'metric':['Temperature']},
    {'start':datetime(2026,9,5),'end':NOW},
    {'start':NOW.replace(hour=9),'end':NOW,'alignment':'clock'},
    {'start':NOW.replace(hour=10,minute=0),'end':NOW.replace(hour=11,minute=0)},
])
def test_contradictions_and_limits_fail(kwargs):
    with pytest.raises(HTTPException) as error:resolve_history_plan(now=NOW,**kwargs)
    assert error.value.status_code==422

def test_limit_caps_full_range_and_does_not_control_lookback():
    plan=resolve_history_plan(window='24h',limit=1000,now=NOW)
    assert len(plan.buckets)==24
    assert resolve_history_plan(limit=1000,now=NOW)==plan
    with pytest.raises(HTTPException) as error:
        resolve_history_plan(window='24h',limit=23,now=NOW)
    assert 'no data was truncated' in error.value.detail

def test_window_final_partial_bucket_and_deduplicated_metrics():
    plan=resolve_history_plan(start=NOW.replace(hour=9),end=NOW.replace(minute=19),interval='5m',alignment='window',metric=['co2','temperature','co2'],now=NOW)
    assert plan.metrics==('co2','temperature')
    assert len(plan.buckets)==13
    assert (plan.buckets[-1][1]-plan.buckets[-1][0]).total_seconds()==120

@pytest.fixture
def client(monkeypatch):
    main.app.dependency_overrides[get_current_user]=lambda:AuthUserRecord(username='fixture',role='teacher',school_id='school_10')
    monkeypatch.setattr(route,'_enforce_environment_device_access',lambda *args:'school_10')
    with TestClient(main.app) as c:yield c
    main.app.dependency_overrides.pop(get_current_user,None)

@pytest.mark.parametrize('query',[
    'window=','aggregate=avg','aggregate=','rolling_1h=true','rolling_1h=false',
    'rolling_24h_hourly=true&rolling_1h=true','rolling_1h=true&metric=co2',
    'rolling_24h_hourly=true&start=2026-09-01T00:00Z&end=2026-09-02T00:00Z',
    'bucket_unit=hour','bucket_size=1','bucket_minutes=5','timezone=Europe/Athens',
    'alignment=window&alignment=clock','limit=1&limit=1000','window=1h&window=2h',
    'start=2026-09-01T00:00Z&end=2026-09-02T00:00Z&window=24h',
    'window=24h&limit=1','start=2026-09-01T00:00:00&end=2026-09-02T00:00:00',
])
def test_http_rejects_ambiguous_controls_before_query(client,monkeypatch,query):
    reader=Mock(side_effect=AssertionError('invalid request reached SQL'))
    monkeypatch.setattr(route,'get_environmental_history',reader)
    assert client.get(BASE+'?'+query).status_code==422
    reader.assert_not_called()

def test_openapi_has_only_canonical_controls():
    params=main.app.openapi()['paths'][BASE.replace('portable-108','{device_id}')]['get']['parameters']
    assert {p['name'] for p in params}=={'device_id','start','end','window','interval','alignment','metric','limit'}

def test_auth_still_required(monkeypatch):
    reader=Mock(side_effect=AssertionError('unauthorized query'))
    monkeypatch.setattr(route,'get_environmental_history',reader)
    with TestClient(main.app) as client:assert client.get(BASE+'?window=1h').status_code==401
    reader.assert_not_called()

@pytest.mark.parametrize('stamp,expected', [
    ('2026-03-14T16:00:00+00:00','2026-03-14T18:00:00+02:00'),
    ('2026-08-14T16:00:00+00:00','2026-08-14T19:00:00+03:00'),
])
def test_http_response_bounds_multiple_metrics_and_athens_offsets(client, monkeypatch, stamp, expected):
    from datetime import timedelta
    from monitoring.services.environment.history import service
    start=datetime.fromisoformat(stamp);end=start+timedelta(hours=1)
    query=Mock(return_value=[
        {'lo':start,'hi':end,'metric':'co2','unit':'ppm','value':600.,'sample_count':8},
        {'lo':start,'hi':end,'metric':'temperature','unit':'C','value':22.,'sample_count':4},
    ])
    monkeypatch.setattr(service,'query_history_rows',query)
    response=client.get(BASE,params=[('start',start.isoformat()),('end',end.isoformat()),
        ('interval','60m'),('metric','co2'),('metric','temperature'),('metric','co2')])
    assert response.status_code==200,response.text
    data=response.json()
    assert data['start']==expected and data['items'][0]['event_time']==expected
    assert data['end']==data['items'][0]['end']
    assert data['interval']=='1h' and data['alignment']=='clock' and data['count']==1
    assert data['items'][0]['measurements']['temperature']=={'value':22.,'unit':'C','sample_count':4}
    assert query.call_args.args[1].metrics==('co2','temperature')

@pytest.mark.parametrize('school,status',[('school_99',403),(None,404)])
def test_device_scope_is_checked_before_sql(monkeypatch,school,status):
    monkeypatch.setitem(main.app.dependency_overrides,get_current_user,
        lambda:AuthUserRecord(username='fixture',role='teacher',school_id='school_10'))
    monkeypatch.setattr(route,'find_school_id_for_device',lambda _:school)
    reader=Mock(side_effect=AssertionError('cross-school request reached SQL'))
    monkeypatch.setattr(route,'get_environmental_history',reader)
    with TestClient(main.app) as client:
        response=client.get(BASE,params={'window':'1h'})
    assert response.status_code==status,response.text
    reader.assert_not_called()


def test_home_adapter_keeps_shared_sql_values_and_shape(monkeypatch):
    from monitoring.services import service_overview
    from monitoring.services.environment.history import service
    def rows(device,plan):
        lo,hi=plan.buckets[-1]
        return [{'lo':lo,'hi':hi,'metric':'temperature','unit':'C','value':28.25,'sample_count':87}]
    monkeypatch.setattr(service,'query_history_rows',rows)
    result=service_overview.get_device_rolling_twenty_four_hour_hourly_series('portable-108')
    assert result.count==1 and result.items[0].measurements['temperature'].value==28.25
    assert set(result.model_dump())=={'device_id','count','items'}
    assert set(result.items[0].model_dump())=={'event_time','device_id','measurements'}
