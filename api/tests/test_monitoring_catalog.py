"""School catalog access and retirement of the duplicate API room catalog."""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from main import app
from monitoring.routes import catalog
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord


@pytest.fixture
def client():
    previous = app.dependency_overrides.pop(get_current_user, None)
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.pop(get_current_user, None)
    if previous is not None:
        app.dependency_overrides[get_current_user] = previous


def identify(role='teacher', school_ids=('school_10',)):
    app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(
        username='catalog-fixture', role=role, school_ids=school_ids
    )


def test_catalog_requires_authentication_before_reading(client):
    with patch.object(catalog, 'schools') as schools:
        assert client.get('/catalog/schools').status_code == 401
    schools.assert_not_called()


@pytest.mark.parametrize('role,scope', [
    ('teacher', ('school_10',)),
    ('municipality', ('school_10', 'school_3')),
    ('system_admin', ()),
])
def test_catalog_preserves_scope_and_payloads(client, role, scope):
    identify(role, scope)
    schools = client.get('/catalog/schools')
    assert schools.status_code == 200
    expected = {s['id'] for s in catalog.schools()} if role == 'system_admin' else set(scope)
    assert {s['id'] for s in schools.json()} == expected


@pytest.mark.parametrize('school_id,room_id,basis,scope', [
    ('school_10', None, 'submeter_sum', 'assumed_school_total'),
    ('school_10', 'teachers', 'submeter_sum', 'metered_loads'),
    ('school_22', None, 'central_meter', 'whole_school'),
    ('school_22', 'teachers', 'submeter_sum', 'metered_loads'),
])
def test_energy_catalog_preserves_consumption_scope(school_id, room_id, basis, scope):
    from monitoring.services.energy_demand.consumption import source_plan

    sources, actual_basis, actual_scope = source_plan(school_id, room_id)
    assert sources
    assert (actual_basis, actual_scope) == (basis, scope)


@pytest.mark.parametrize('path,count', [
    ('/indoor_environment/schools/school_10/devices', 5),
    ('/energy/schools/school_10/devices', 8),
])
def test_merged_catalog_preserves_flat_device_responses(client, path, count):
    identify()
    response = client.get(path)
    assert response.status_code == 200
    devices = response.json()
    assert len(devices) == count
    assert all('id' in device and 'room_id' in device for device in devices)
    assert all('environment' not in device and 'energy' not in device for device in devices)


def test_openapi_only_advertises_canonical_catalog(client):
    paths = client.get('/openapi.json').json()['paths']
    assert '/schools' not in paths and '/rooms' not in paths
    assert '/catalog/schools' in paths
    assert '/catalog/schools/{school_id}/rooms' not in paths


@pytest.mark.parametrize('path', [
    '/schools', '/rooms?school_id=school_10',
    '/catalog/schools/school_10/rooms',
    '/catalog/schools/school_3/rooms?school_id=school_10',
    '/catalog/schools/unknown/rooms',
])
@pytest.mark.parametrize('authenticated', [False, True])
def test_retired_catalog_routes_are_not_available(client, path, authenticated):
    if authenticated:
        identify()
    with patch.object(catalog, 'schools') as schools, patch(
        'main.get_connection', side_effect=AssertionError('Retired catalog route read the database')
    ):
        assert client.get(path).status_code == 404
    schools.assert_not_called()


def test_catalog_cors_preflight(client):
    result = client.options('/catalog/schools', headers={
        'Origin': 'https://schoolheroz.com',
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'authorization',
    })
    assert result.status_code == 200
    assert result.headers['access-control-allow-origin'] == 'https://schoolheroz.com'
