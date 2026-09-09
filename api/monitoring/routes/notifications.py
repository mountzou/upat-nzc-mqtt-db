"""Global notification history for the authenticated account's school scope."""
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from psycopg2 import Error as DatabaseError

from monitoring.notifications import repository
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord

router = APIRouter(prefix='/notifications', tags=['notifications'])


def unavailable():
    return HTTPException(503, 'Notifications are temporarily unavailable.',
                         headers={'Cache-Control': 'no-store'})


@router.get('')
def list_notifications(
    response: Response,
    user: Annotated[AuthUserRecord, Depends(get_current_user)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    before_id: Annotated[int | None, Query(ge=1)] = None,
):
    response.headers['Cache-Control'] = 'no-store'
    try:
        return repository.list_notifications(user, limit=limit, before_id=before_id)
    except DatabaseError:
        raise unavailable() from None


@router.patch('/{notification_id}/read')
def mark_read(
    notification_id: Annotated[int, Path(ge=1)],
    response: Response,
    user: Annotated[AuthUserRecord, Depends(get_current_user)],
):
    response.headers['Cache-Control'] = 'no-store'
    try:
        receipt = repository.mark_read(user, notification_id)
    except DatabaseError:
        raise unavailable() from None
    if receipt is None:
        raise HTTPException(404, 'Notification not found.', headers={'Cache-Control': 'no-store'})
    return receipt
