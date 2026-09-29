"""Public authenticated monitoring surface; EnergyPlus stays on its own backend."""
from fastapi import Depends, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from monitoring.routes import auth, catalog, energy_demand, indoor_environment, production, notifications
from monitoring.services import authentication

class SessionRequest(BaseModel):
    access_token: str=Field(min_length=1,max_length=8192)

def install(app, service_dependency):
    app.add_middleware(CORSMiddleware,
        allow_origins=['https://schoolheroz.com','https://www.schoolheroz.com','https://upat-nzc-energyplus.vercel.app'],
        allow_origin_regex=r'https?://(localhost|127\.0\.0\.1)(:\d+)?',
        allow_methods=['GET','POST','PATCH','OPTIONS'],allow_headers=['Authorization','Content-Type','Accept'],
        expose_headers=['Server-Timing','Retry-After'],allow_credentials=True)
    for router in [auth.router,catalog.router,energy_demand.router,indoor_environment.router,production.router,notifications.router]:
        app.include_router(router)

    # Render can validate VPS-issued sessions for EnergyPlus through the existing
    # protected server boundary. No signing key or database password leaves VPS.
    @app.post('/internal/auth/session', dependencies=[Depends(service_dependency)])
    def verify_session(payload:SessionRequest,response:Response):
        response.headers['Cache-Control']='no-store'
        try: user=authentication.user_from_access_token(payload.access_token)
        except authentication.VpsAuthUnavailableError: raise HTTPException(503,'Authentication is unavailable') from None
        if user is None: raise auth.unauthorized()
        return {**authentication.public_user(user),'token_version':user.token_version}
