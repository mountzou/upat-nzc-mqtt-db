from datetime import date, timedelta
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from monitoring.routes.auth import get_current_user
from monitoring.services.energy_production.aggregates import get_solar_production, SolarProductionResponse

router=APIRouter()

@router.get('/energy/production/solar/aggregates', response_model=SolarProductionResponse)
def aggregates(response: Response, start_date: date, end_date: date,
               resolution: Literal['day','hour'], user=Depends(get_current_user)):
    response.headers['Cache-Control']='private, no-store'
    try: return get_solar_production(start_date,end_date,resolution)
    except ValueError as exc: raise HTTPException(400,str(exc)) from exc

@router.get('/energy/production/solar/day-ahead-forecasts')
def forecasts(start_date: date, end_date: date, user=Depends(get_current_user)):
    import main
    from fastapi.encoders import jsonable_encoder
    payload=jsonable_encoder(main.get_pv_day_ahead_forecast_range(start_date,end_date))
    return {'collection':'pv_day_ahead_forecast_hourly','source':'postgres',
            'start_date':start_date.isoformat(),'end_date':end_date.isoformat(),
            'count':len(payload['forecasts']),'forecasts':payload['forecasts']}

@router.get('/energy/production/solar/active-power')
def legacy_actuals(days: int=Query(10,ge=1,le=30),start_date:date|None=None,end_date:date|None=None,
                   user=Depends(get_current_user)):
    """Compatibility for the current web chart; the native client uses aggregates."""
    import main
    from monitoring.services.energy_production.aggregates import date_range
    if (start_date is None)!=(end_date is None): raise HTTPException(400,'Provide both dates')
    if start_date is None:
        bounds=main.get_pv_readings_bounds()
        latest=bounds.get('max_date')
        if not latest:
            return {'collection':'pv_plant_readings_5m','source':'postgres','latest_observed_at':None,
                    'start_date':'','end_date':'','count':0,'points':[]}
        end_date=date.fromisoformat(latest) if isinstance(latest,str) else latest
        start_date=end_date-timedelta(days=days-1)
    try: dates=date_range(start_date,end_date)
    except ValueError as exc: raise HTTPException(400,str(exc)) from exc
    raw=main.get_pv_readings(start_date,end_date)
    points=[{'timestamp':p['observed_at'].isoformat(),'value':p['active_power_kw']} for p in raw['points']]
    return {'collection':'pv_plant_readings_5m','source':'postgres',
            'latest_observed_at':points[-1]['timestamp'] if points else None,
            'start_date':start_date.isoformat(),'end_date':end_date.isoformat(),'count':len(points),'points':points}
