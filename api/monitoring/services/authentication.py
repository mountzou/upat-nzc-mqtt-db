"""End-user sessions owned by the VPS, backed by the existing app_users table."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import os
import jwt
from jwt import InvalidTokenError
from fastapi import HTTPException
from auth_service import AuthRepository, _public_user, _DUMMY_PASSWORD_HASH, verify_password

AUTH_TOKEN_ALGORITHM = 'HS256'
AUTH_TOKEN_AUDIENCE = 'schoolheroz-web'
AUTH_TOKEN_ISSUER = 'schoolheroz-vps'

class VpsAuthUnavailableError(RuntimeError): pass
class VpsAuthRateLimitedError(RuntimeError):
    def __init__(self, retry_after_seconds): self.retry_after_seconds=retry_after_seconds

@dataclass(frozen=True)
class AuthUserRecord:
    username: str
    role: str
    school_id: str | None = None
    municipality_id: str | None = None
    school_ids: tuple[str,...] = ()
    token_version: int = 1
    theme: str = 'light'
    onboarding_completed: tuple[str,...] = ()

def repository():
    import main
    return AuthRepository(main.get_connection)

def signing_secret():
    secret=os.getenv('AUTH_TOKEN_SECRET','').strip()
    if len(secret)<32:
        raise VpsAuthUnavailableError('Session signing is not configured')
    return secret

def record(row):
    identity=_public_user(row)
    identity['school_ids']=tuple(identity['school_ids'])
    identity['onboarding_completed']=tuple(identity['onboarding_completed'])
    return AuthUserRecord(**identity)

def public_user(user):
    return {key:value for key,value in user.__dict__.items() if key!='token_version'}

def authenticate_user(username, password):
    import main
    signing_secret()
    username=username.strip()
    main.AUTH_VERIFY_RATE_LIMITER.check(username)
    try:
        repo=repository(); row=repo.fetch_user(username,include_password_hash=True)
        valid=verify_password(password,row.get('password_hash') if row else _DUMMY_PASSWORD_HASH)
        if not valid or not row or not row.get('is_active'): return None
        record(row)
        updated=repo.record_successful_login(username)
        return record(updated) if updated else None
    except HTTPException: raise
    except Exception as exc: raise VpsAuthUnavailableError('Authentication is unavailable') from exc

def create_access_token(user):
    ttl=max(300,min(int(os.getenv('AUTH_TOKEN_TTL_SECONDS','28800')),86400))
    now=datetime.now(timezone.utc)
    payload={**public_user(user),'token_version':user.token_version,'sub':user.username,
             'iss':AUTH_TOKEN_ISSUER,'aud':AUTH_TOKEN_AUDIENCE,'iat':now,
             'exp':now+timedelta(seconds=ttl),'jti':str(uuid4())}
    payload.pop('username');payload.pop('theme');payload.pop('onboarding_completed')
    return jwt.encode(payload,signing_secret(),algorithm=AUTH_TOKEN_ALGORITHM),ttl

def user_from_access_token(token):
    if not isinstance(token,str) or len(token)>8192: return None
    try:
        payload=jwt.decode(token,signing_secret(),algorithms=[AUTH_TOKEN_ALGORITHM],
            audience=AUTH_TOKEN_AUDIENCE,issuer=AUTH_TOKEN_ISSUER,
            options={'require':['sub','role','school_ids','token_version','iat','exp','jti']})
    except InvalidTokenError: return None
    username=payload.get('sub')
    if not isinstance(username,str) or not username.strip(): return None
    try:
        row=repository().fetch_user(username,include_password_hash=False)
        if not row or not row.get('is_active'): return None
        user=record(row)
    except Exception as exc: raise VpsAuthUnavailableError('Authentication is unavailable') from exc
    for key in ['role','school_id','municipality_id','school_ids','token_version']:
        expected=getattr(user,key)
        if key=='school_ids': expected=list(expected)
        if payload.get(key)!=expected: return None
    if type(payload['token_version']) is not int: return None
    return user

def update_user_preferences(username, **kwargs):
    from auth_service import UserPreferencesRequest
    try:
        row=repository().update_user_preferences(UserPreferencesRequest(username=username,**kwargs))
        return record(row) if row else None
    except Exception as exc: raise VpsAuthUnavailableError('Preferences are unavailable') from exc
