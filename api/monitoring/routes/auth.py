from collections.abc import Callable
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, model_validator

from monitoring.services.authentication import (
    AuthUserRecord,
    VpsAuthRateLimitedError,
    VpsAuthUnavailableError,
    authenticate_user,
    create_access_token,
    public_user,
    update_user_preferences,
    user_from_access_token,
)


router = APIRouter(prefix="/auth", tags=["authentication"])
bearer_scheme = HTTPBearer(auto_error=False)


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=1, max_length=256)


class AuthenticatedUserResponse(BaseModel):
    username: str
    role: Literal["teacher", "municipality", "system_admin"]
    school_id: str | None = None
    municipality_id: str | None = None
    school_ids: list[str] = Field(default_factory=list)
    theme: Literal["light", "dark"]
    onboarding_completed: list[str] = Field(default_factory=list)


class UserPreferencesRequest(BaseModel):
    theme: Literal["light", "dark"] | None = None
    onboarding_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=120,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    onboarding_completed: bool | None = None

    @model_validator(mode="after")
    def validate_patch(self):
        has_onboarding_patch = self.onboarding_key is not None
        if has_onboarding_patch != (self.onboarding_completed is not None):
            raise ValueError(
                "onboarding_key and onboarding_completed must be provided together"
            )
        if self.theme is None and not has_onboarding_patch:
            raise ValueError("at least one preference must be provided")
        return self


class LoginResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int
    user: AuthenticatedUserResponse


def unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired authentication credentials.",
        headers={
            "WWW-Authenticate": "Bearer",
            "Cache-Control": "no-store",
        },
    )


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> AuthUserRecord:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise unauthorized()
    try:
        user = user_from_access_token(credentials.credentials)
    except VpsAuthUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is temporarily unavailable.",
            headers={"Cache-Control": "no-store"},
        ) from exc
    if user is None:
        raise unauthorized()
    return user


def require_roles(*allowed_roles: str) -> Callable[..., AuthUserRecord]:
    allowed = frozenset(allowed_roles)
    if not allowed:
        raise ValueError("At least one allowed role is required.")

    def require_allowed_role(
        user: AuthUserRecord = Depends(get_current_user),
    ) -> AuthUserRecord:
        if user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have permission to access this resource.",
            )
        return user

    return require_allowed_role


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, response: Response):
    response.headers["Cache-Control"] = "no-store"
    try:
        user = authenticate_user(payload.username, payload.password)
    except VpsAuthRateLimitedError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many authentication attempts. Try again later.",
            headers={
                "Cache-Control": "no-store",
                "Retry-After": str(exc.retry_after_seconds),
            },
        ) from exc
    except VpsAuthUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is temporarily unavailable.",
            headers={"Cache-Control": "no-store"},
        ) from exc
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
            headers={
                "WWW-Authenticate": "Bearer",
                "Cache-Control": "no-store",
            },
        )
    token, expires_in = create_access_token(user)
    return {
        "access_token": token,
        "expires_in": expires_in,
        "user": public_user(user),
    }


@router.get("/me", response_model=AuthenticatedUserResponse)
def read_current_user(
    response: Response,
    user: AuthUserRecord = Depends(get_current_user),
):
    response.headers["Cache-Control"] = "no-store"
    return public_user(user)


@router.patch("/preferences", response_model=AuthenticatedUserResponse)
def patch_preferences(
    payload: UserPreferencesRequest,
    response: Response,
    user: AuthUserRecord = Depends(get_current_user),
):
    response.headers["Cache-Control"] = "no-store"
    try:
        updated_user = update_user_preferences(
            user.username,
            theme=payload.theme,
            onboarding_key=payload.onboarding_key,
            onboarding_completed=payload.onboarding_completed,
        )
    except VpsAuthUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is temporarily unavailable.",
            headers={"Cache-Control": "no-store"},
        ) from exc
    if updated_user is None:
        raise unauthorized()
    return public_user(updated_user)
