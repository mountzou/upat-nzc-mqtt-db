"""Authenticated, allowlisted HTTP reads for trusted backend services.

The original handlers remain available during the caller migration. These
routes reuse them directly; they introduce no second query implementation.
"""
import hmac
import os
from collections.abc import Iterable

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


DATA_SERVICE_PATHS = (
    "/health",
    "/upat/device/{device_id}/latest",
    "/upat/device/{device_id}/history",
    "/shelly/hourly-energy",
    "/weather/hourly/forecast",
    "/simulations/day-ahead/latest",
    "/pv/day-ahead/latest",
)

_bearer = HTTPBearer(auto_error=False, scheme_name="DataServiceToken")


def require_data_service_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> None:
    configured = os.getenv("DATA_SERVICE_TOKEN", "").strip()
    if len(configured) < 32:
        raise HTTPException(
            503, "Data service authentication is not configured",
            headers={"Cache-Control": "no-store"},
        )
    if (
        len(request.headers.getlist("authorization")) != 1
        or credentials is None
        or not hmac.compare_digest(
            credentials.credentials.encode("utf-8"), configured.encode("utf-8")
        )
    ):
        raise HTTPException(
            401, "Invalid or missing data service credentials",
            headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
        )


def build_data_service_router(routes: Iterable[object]) -> APIRouter:
    """Expose only the reviewed GET handlers; never proxy an arbitrary path."""
    sources = [route for route in routes if isinstance(route, APIRoute)]
    router = APIRouter(
        prefix="/internal/data", tags=["internal-data"],
        dependencies=[Depends(require_data_service_token)],
    )
    for path in DATA_SERVICE_PATHS:
        matches = [r for r in sources if r.path == path and r.methods == {"GET"}]
        if len(matches) != 1:
            raise RuntimeError(f"Expected one reviewed data reader for {path}")
        source = matches[0]
        router.add_api_route(
            path, source.endpoint, methods=["GET"],
            name=f"data_service_{source.name}",
            dependencies=source.dependencies,
            response_model=source.response_model,
            status_code=source.status_code,
            response_class=source.response_class,
            response_description=source.response_description,
            responses=source.responses,
            description=source.description,
            response_model_exclude_none=source.response_model_exclude_none,
            response_model_exclude_unset=source.response_model_exclude_unset,
            response_model_exclude_defaults=source.response_model_exclude_defaults,
            response_model_by_alias=source.response_model_by_alias,
        )
    return router
