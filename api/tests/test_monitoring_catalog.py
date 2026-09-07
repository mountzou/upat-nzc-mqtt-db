"""Catalog URL migration preserves data, authentication and school scope."""
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


@pytest.mark.parametrize('path', [
    '/catalog/schools', '/catalog/schools/school_10/rooms',
])
def test_catalog_requires_authentication_before_reading(client, path):
    with patch.object(catalog, 'schools') as schools, patch.object(catalog, 'rooms') as rooms:
        assert client.get(path).status_code == 401
    schools.assert_not_called()
    rooms.assert_not_called()


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
    rooms = client.get('/catalog/schools/school_10/rooms')
    assert rooms.status_code == 200
    assert rooms.json()
    assert all(r['school_id'] == 'school_10' for r in rooms.json())


@pytest.mark.parametrize('path', [
    '/catalog/schools/school_3/rooms',
    '/catalog/schools/school_3/rooms?school_id=school_10',
])
def test_room_scope_is_checked_before_catalog_read(client, path):
    identify()
    with patch.object(catalog, 'rooms') as rooms:
        assert client.get(path).status_code == 403
    rooms.assert_not_called()


def test_unknown_school_is_not_found_for_admin(client):
    identify('system_admin', ())
    assert client.get('/catalog/schools/unknown/rooms').status_code == 404


def test_openapi_only_advertises_canonical_catalog(client):
    paths = client.get('/openapi.json').json()['paths']
    assert '/schools' not in paths and '/rooms' not in paths
    assert '/catalog/schools' in paths
    parameters = paths['/catalog/schools/{school_id}/rooms']['get']['parameters']
    school = next(p for p in parameters if p['name'] == 'school_id')
    assert school['in'] == 'path' and school['required'] is True


def test_catalog_cors_preflight(client):
    result = client.options('/catalog/schools/school_10/rooms', headers={
        'Origin': 'https://schoolheroz.com',
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'authorization',
    })
    assert result.status_code == 200
    assert result.headers['access-control-allow-origin'] == 'https://schoolheroz.com'


@pytest.mark.parametrize('path', ['/schools', '/rooms?school_id=school_10'])
@pytest.mark.parametrize('authenticated', [False, True])
def test_retired_catalog_aliases_are_not_available(client, path, authenticated):
    if authenticated:
        identify()
    with patch.object(catalog, 'schools') as schools, patch.object(catalog, 'rooms') as rooms:
        assert client.get(path).status_code == 404
    schools.assert_not_called()
    rooms.assert_not_called()
