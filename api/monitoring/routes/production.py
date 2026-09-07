from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from monitoring.routes.auth import get_current_user
from monitoring.services.energy_production.energy import get_energy, latest_forecast_date, ProductionEnergyResponse

router=APIRouter()

def validate_energy_query(request: Request, response: Response, *, forecasts=False):
    allowed = {'start_date', 'end_date', 'interval'} | ({'latest'} if forecasts else set())
    if set(request.query_params) - allowed or any(len(request.query_params.getlist(k)) != 1 for k in request.query_params):
        raise HTTPException(422, 'Use only start_date, end_date and interval' + (' or latest=true with interval' if forecasts else ''))
    response.headers['Cache-Control'] = 'private, no-store'


@router.get('/energy/production/history', response_model=ProductionEnergyResponse)
def history(request: Request, response: Response, start_date: date, end_date: date,
            interval: str = '1h', user=Depends(get_current_user)):
    validate_energy_query(request, response)
    try:
        return get_energy(start_date, end_date, interval, 'history')
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get('/energy/production/forecasts', response_model=ProductionEnergyResponse)
def energy_forecasts(request: Request, response: Response, start_date: date | None = None,
                     end_date: date | None = None, interval: str = '1h', latest: bool = False,
                     user=Depends(get_current_user)):
    validate_energy_query(request, response, forecasts=True)
    if latest:
        if start_date is not None or end_date is not None:
            raise HTTPException(422, 'Use latest=true without dates')
        start_date = end_date = latest_forecast_date()
    elif start_date is None or end_date is None:
        raise HTTPException(422, 'Provide both start_date and end_date, or latest=true')
    try:
        return get_energy(start_date, end_date, interval, 'forecast')
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
