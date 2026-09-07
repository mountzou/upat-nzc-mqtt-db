"""Canonical public history remains stable after removing the compatibility code."""
import os
import subprocess
import sys
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
import main
from monitoring.routes import indoor_environment as route
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.environment.history import service
from monitoring.services import service_overview

BASE = '/indoor_environment/devices/portable-108/history'
HEADER = 'X-Environmental-History-Contract'

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setitem(main.app.dependency_overrides, get_current_user,
                        lambda: AuthUserRecord(username='fixture', role='system_admin'))
    monkeypatch.setattr(route, '_enforce_environment_device_access', lambda *a: 'school_10')
    with TestClient(main.app) as result:
        yield result

@pytest.mark.parametrize('query', [
    'aggregate=avg&interval=1m&limit=1', 'aggregate=avg&interval=day&limit=7',
    'rolling_24h_hourly=true', 'rolling_24h_hourly=false', 'rolling_24h_hourly=',
    'rolling_1h=', 'rolling_1h=invalid', 'window=1h&aggregate=avg',
    'alignment=clock&rolling_1h=false', 'rolling_1h=true&rolling_24h_hourly=true',
    'rolling_1h=true&metric=temperature', 'rolling_24h_hourly=true&limit=24',
    'rolling_1h=true&interval=1h', 'aggregate=sum', 'aggregate=avg&limit=1&limit=2',
])
def test_old_requests_rejected_before_database(client, monkeypatch, query):
    database = Mock(side_effect=AssertionError('retired request reached database'))
    monkeypatch.setattr(main, 'get_connection', database)
    assert client.get(BASE + '?' + query).status_code == 422
    database.assert_not_called()

@pytest.mark.parametrize('query', [
    'window=1m&interval=1m&alignment=window&limit=1',
    'window=1h&interval=1h&alignment=window&limit=1',
    'window=24h&interval=1h&alignment=clock&limit=24',
    'window=15m&interval=1m&alignment=window&limit=15&metric=temperature',
    'alignment=window&interval=1h&start=2026-10-24T21:00:00Z&end=2026-10-31T22:00:00Z&limit=169',
    'alignment=clock&interval=day&start=2026-09-01T21:00:00Z&end=2026-09-03T21:00:00Z&limit=2',
])
def test_migrated_consumer_queries_keep_header_schema_and_empty_success(client, monkeypatch, query):
    monkeypatch.setattr(service, 'query_history_rows', lambda *a: [])
    response = client.get(BASE + '?' + query)
    assert response.status_code == 200, response.text
    assert response.headers[HEADER] == 'canonical'
    assert set(response.json()) == {'device_id', 'start', 'end', 'interval', 'alignment', 'limit', 'count', 'items'}
    assert response.json()['count'] == 0 and response.json()['items'] == []

@pytest.mark.parametrize('status', [404, 422, 503])
def test_canonical_errors_never_use_the_retained_internal_reader(client, monkeypatch, status):
    monkeypatch.setattr(route, 'get_environmental_history', Mock(side_effect=HTTPException(status, 'fixture')))
    internal = Mock(side_effect=AssertionError('error fell back to internal reader'))
    monkeypatch.setattr(service_overview, 'get_device_history', internal)
    assert client.get(BASE + '?window=1h').status_code == status
    internal.assert_not_called()


def test_canonical_usage_log_has_no_identity(client, monkeypatch, caplog):
    monkeypatch.setattr(service, 'query_history_rows', lambda *a: [])
    with caplog.at_level('INFO', logger=route.logger.name):
        assert client.get(BASE + '?window=1h').status_code == 200
    assert 'environment_history contract=canonical preset=history' in caplog.text
    assert 'portable-108' not in caplog.text and 'fixture' not in caplog.text

@pytest.mark.parametrize('obsolete_value', ['true', 'false', 'ture'])
def test_obsolete_environment_setting_cannot_restore_legacy_on_startup(obsolete_value):
    # Real fresh imports: a stale VPS env/rollback overlay must not reactivate it.
    code = '''
import main
from fastapi.testclient import TestClient
from monitoring.routes import indoor_environment as route
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.environment.history import service
main.app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(username='fixture', role='system_admin')
route._enforce_environment_device_access = lambda *a: 'school_10'
service.query_history_rows = lambda *a: []
with TestClient(main.app) as c:
    for q in ('aggregate=avg', 'rolling_1h=true', 'rolling_24h_hourly=true'):
        assert c.get('/indoor_environment/devices/fixture/history?' + q).status_code == 422
    r = c.get('/indoor_environment/devices/fixture/history')
    assert r.status_code == 200 and r.headers['X-Environmental-History-Contract'] == 'canonical'
'''
    result = subprocess.run([sys.executable, '-c', code],
                            env={**os.environ, 'ENVIRONMENT_HISTORY_LEGACY_COMPAT': obsolete_value},
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
