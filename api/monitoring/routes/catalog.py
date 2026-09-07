from functools import lru_cache
import json
from fastapi import APIRouter, Depends, HTTPException
from monitoring.config import DIR_ROOMS
from monitoring.routes.auth import get_current_user
from monitoring.services.authorization import enforce_school_access, filter_accessible_schools
from monitoring.schemas import SchoolMetadata, RoomMetadata, RoomCatalogEntry

router=APIRouter()

@lru_cache(maxsize=1)
def schools():
    return [SchoolMetadata.model_validate(s).model_dump() for s in json.loads((DIR_ROOMS/'schools.json').read_text())]

@lru_cache(maxsize=32)
def rooms(school_id):
    if school_id not in {s['id'] for s in schools()}: raise HTTPException(404,'School not found')
    result=[]
    for raw in json.loads((DIR_ROOMS/school_id/'rooms.json').read_text()):
        room=RoomCatalogEntry.model_validate(raw); data=room.model_dump()
        exists=bool(raw.get('idf_exists')); available=room.simulation_enabled and exists
        result.append({**data,'school_id':school_id,'idf_path':'','idf_exists':exists,
            'simulatable':available,'simulation_status':'available' if available else 'disabled' if not room.simulation_enabled else 'missing_idf',
            'supports':{'occupancy':True,'heating_setpoint':True,'cooling_setpoint':room.thermostat_type=='dual_setpoint',
                        'lighting_w_per_m2':True,'infiltration_ach':room.defaults.infiltration_ach is not None}})
    return result

@router.get('/catalog/schools',response_model=list[SchoolMetadata],tags=['catalog'])
def get_schools(user=Depends(get_current_user)):
    return filter_accessible_schools(user,schools())

@router.get('/catalog/schools/{school_id}/rooms',response_model=list[RoomMetadata],tags=['catalog'])
def get_rooms(school_id:str,user=Depends(get_current_user)):
    enforce_school_access(user,school_id)
    return rooms(school_id)
