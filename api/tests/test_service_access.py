"""Scope, fail-closed authentication, and unchanged readers for service access."""
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import main
from monitoring.service_access import DATA_SERVICE_PATHS, build_data_service_router

TOKEN = "fixture-data-service-" + "a" * 40


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("DATA_SERVICE_TOKEN", TOKEN)
    return TestClient(main.app)


@pytest.mark.parametrize("path", DATA_SERVICE_PATHS)
@pytest.mark.parametrize("headers", [
    {}, {"Authorization": "Bearer wrong"},
    {"Authorization": "Basic " + TOKEN},
    {"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.user-session"},
])
def test_unauthorized_reads_never_reach_database(client, monkeypatch, path, headers):
    connection = Mock(side_effect=AssertionError("unauthorized request reached DB"))
    monkeypatch.setattr(main, "get_connection", connection)
    response = client.get("/internal/data" + path.replace("{device_id}", "fixture"), headers=headers)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["www-authenticate"] == "Bearer"
    assert TOKEN not in response.text
    connection.assert_not_called()


@pytest.mark.parametrize("configured", ["", "too-short", " " * 40])
def test_missing_configuration_is_closed(client, monkeypatch, configured):
    monkeypatch.setenv("DATA_SERVICE_TOKEN", configured)
    response = client.get("/internal/data/health", headers={"Authorization": "Bearer " + TOKEN})
    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"


def test_duplicate_headers_and_query_credentials_are_rejected(client):
    response = client.get("/internal/data/health", headers=[
        ("Authorization", "Bearer " + TOKEN), ("Authorization", "Bearer " + TOKEN),
    ])
    assert response.status_code == 401
    assert client.get("/internal/data/health", params={"token": TOKEN}).status_code == 401


@pytest.mark.parametrize("path", [
    "/upat/devices", "/shelly/devices", "/shelly/energy",
    "/shelly/device/fixture/energy", "/shelly/device/fixture/latest",
    "/ops/telemetry", "/internal/auth/verify", "/pv/readings",
])
def test_unreviewed_paths_are_not_exposed(client, path):
    assert client.get("/internal/data" + path, headers={"Authorization": "Bearer " + TOKEN}).status_code == 404


def test_data_credential_is_not_a_user_session(client, monkeypatch):
    monkeypatch.setenv("AUTH_TOKEN_SECRET", "fixture-user-signing-" + "b" * 40)
    assert client.get("/catalog/schools", headers={"Authorization": "Bearer " + TOKEN}).status_code == 401


def test_service_surface_is_get_only(client):
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)("/internal/data/health", headers={"Authorization": "Bearer " + TOKEN}).status_code == 405


def test_supported_reader_preserves_handler_arguments_and_response(monkeypatch):
    monkeypatch.setenv("DATA_SERVICE_TOKEN", TOKEN)
    original = FastAPI()
    calls = []

    def reader(device_id: str = "none", limit: int = 3):
        calls.append((device_id, limit))
        return {"device_id": device_id, "limit": limit, "value": None}

    for path in DATA_SERVICE_PATHS:
        original.add_api_route(path, reader, methods=["GET"])
    original.include_router(build_data_service_router(original.routes))
    with TestClient(original) as client:
        expected = client.get("/upat/device/fixture/history", params={"limit": 7})
        actual = client.get("/internal/data/upat/device/fixture/history", params={"limit": 7},
                            headers={"Authorization": "Bearer " + TOKEN})
        assert actual.status_code == expected.status_code == 200
        assert actual.json() == expected.json()
        assert calls == [("fixture", 7), ("fixture", 7)]
        assert client.get("/internal/data/health", params={"limit": "invalid"},
                          headers={"Authorization": "Bearer " + TOKEN}).status_code == 422


def test_router_requires_all_reviewed_readers():
    with pytest.raises(RuntimeError, match="Expected one reviewed data reader"):
        build_data_service_router([])


def test_openapi_parameters_match_original_and_require_distinct_security():
    schema = main.app.openapi()
    for path in DATA_SERVICE_PATHS:
        original = schema["paths"][path]["get"]
        protected = schema["paths"]["/internal/data" + path]["get"]
        assert protected.get("parameters", []) == original.get("parameters", [])
        assert protected["responses"] == original["responses"]
        assert protected["security"] == [{"DataServiceToken": []}]
    assert schema["components"]["securitySchemes"]["DataServiceToken"]["scheme"] == "bearer"


def test_router_does_not_register_write_handlers():
    original = FastAPI()
    original.add_api_route("/health", lambda: {}, methods=["POST"])
    with pytest.raises(RuntimeError):
        build_data_service_router(original.routes)
