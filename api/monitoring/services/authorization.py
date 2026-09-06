from fastapi import HTTPException, status

from monitoring.services.authentication import AuthUserRecord


def can_access_school(user: AuthUserRecord, school_id: str) -> bool:
    normalized_school_id = str(school_id).strip()
    if not normalized_school_id:
        return False
    if user.role == "system_admin":
        return True
    return normalized_school_id in user.school_ids


def enforce_school_access(user: AuthUserRecord, school_id: str) -> None:
    if can_access_school(user, school_id):
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You do not have permission to access this school.",
    )


def filter_accessible_schools(
    user: AuthUserRecord,
    schools: list[dict],
) -> list[dict]:
    if user.role == "system_admin":
        return schools
    return [school for school in schools if can_access_school(user, school.get("id", ""))]
