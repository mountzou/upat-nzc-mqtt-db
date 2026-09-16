"""API-only deployment scope and automatic rollback, without VPS access."""
import importlib.util
import json
from pathlib import Path
import pytest
from test_postgres_volume_compose import model

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('stage2',ROOT/'ops/shelly-compact/stage2.py')
s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)


def test_only_api_image_and_two_read_flags_change():
    before=model(ROOT/'ops/shelly-compact/compose.stage1.prod.yml')
    after=model(ROOT/'ops/shelly-compact/compose.stage2.prod.yml')
    assert after==s.expected_after(before)
    s.s.validate_model(after)
    assert after['services']['shelly-ingestor']==before['services']['shelly-ingestor']


@pytest.mark.parametrize('name',['iot_postgres','shelly_ingestor','ttn_ingestor','iot_caddy','iot_mosquitto'])
def test_unrelated_runtime_drift_rejected(name):
    before={k:{'id':k} for k in s.s.NAMES}
    after={**before,name:{'id':'changed'}}
    with pytest.raises(RuntimeError,match='Unrelated container changed'):
        s.unchanged_containers(before,after)


def test_failed_active_probe_restores_only_api(tmp_path,monkeypatch):
    cfg=tmp_path/'docker-compose.prod.yml';cfg.write_text('old')
    candidate=tmp_path/'candidate.yml';candidate.write_text('new')
    receipts=tmp_path/'receipts';receipts.mkdir()
    baseline={'iot_api':{'id':'old'}}
    (receipts/'before.json').write_text(json.dumps({'containers':baseline,'compose_sha':s.sha(cfg)}))
    (receipts/'probe.json').write_text(json.dumps({'status':'PASS','history_cases':[{'name':'fixture','path':'/fixture','expected':{}}]}))
    (receipts/'compose.before.yml').write_text('old')
    monkeypatch.setattr(s,'CANONICAL',cfg);monkeypatch.setattr(s,'CANDIDATE',candidate)
    monkeypatch.setattr(s,'RECEIPTS',receipts)
    monkeypatch.setattr(s,'artifacts',lambda:None);monkeypatch.setattr(s,'guard',lambda:None)
    monkeypatch.setattr(s.s,'snapshot',lambda:baseline)
    monkeypatch.setattr(s.s,'model',lambda _: {})
    monkeypatch.setattr(s,'expected_after',lambda _: {})
    monkeypatch.setattr(s.s,'atomic_config',lambda data:cfg.write_bytes(data))
    commands=[]
    monkeypatch.setattr(s.subprocess,'run',lambda args,**kw:commands.append(args))
    monkeypatch.setattr(s,'ready',lambda _:None)
    def inspect(_):
        return {'Image':s.IMAGE if cfg.read_text()=='new' else s.OLD_IMAGE,
                'Config':{'Env':['SHELLY_MEASUREMENTS_READ_STORAGE=compact','SHELLY_MEASUREMENTS_ROUNDING=decimal_1']}}
    monkeypatch.setattr(s.s,'inspect',inspect)
    def fail(*args,**kwargs):raise RuntimeError('Active response mismatch')
    monkeypatch.setattr(s,'fetch',fail)
    with pytest.raises(RuntimeError,match='Active response mismatch'):s.activate()
    assert cfg.read_text()=='old'
    assert len(commands)==2
    for cmd in commands:
        assert cmd[-1]=='api' and '--no-deps' in cmd and '--no-build' in cmd
        assert not any(x in cmd for x in ['down','restart','shelly-ingestor','iot_postgres'])
    assert json.loads((receipts/'rollback.json').read_text())['original_api_restored']
