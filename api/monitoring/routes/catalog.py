from functools import lru_cache
import json
from fastapi import APIRouter, Depends
from monitoring.config import DIR_ROOMS
from monitoring.routes.auth import get_current_user
from monitoring.services.authorization import filter_accessible_schools
from monitoring.schemas import SchoolMetadata

router=APIRouter()

@lru_cache(maxsize=1)
def schools():
    return [SchoolMetadata.model_validate(s).model_dump() for s in json.loads((DIR_ROOMS/'schools.json').read_text())]

@router.get('/catalog/schools',response_model=list[SchoolMetadata],tags=['catalog'])
def get_schools(user=Depends(get_current_user)):
    return filter_accessible_schools(user,schools())
