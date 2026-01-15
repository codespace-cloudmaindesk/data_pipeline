from fastapi import APIRouter
from uuid import UUID
from datetime import date
from src.services.analytics_service import analytics_service

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/branch/{branch_id}/kpis")
async def get_branch_kpis(branch_id: UUID, snapshot_date: date):
    """
    Returns branch-level KPIs for a given date.
    """
    return await analytics_service.branch_kpis(branch_id, snapshot_date)
