from datetime import date
from uuid import UUID
from src.repositories import scylla_repo

class AnalyticsService:

    async def branch_kpis(self, branch_id: UUID, snapshot_date: date):
        """
        Return KPIs for a branch on a given date.
        Currently: total stock, total quantity
        """
        session = scylla_repo.get_session()
        row = session.execute(
            """
            SELECT total_quantity, total_stock_value
            FROM inventory_snapshot_by_branch_day
            WHERE snapshot_date=%s AND branch_id=%s
            """,
            (snapshot_date, branch_id)
        ).one()

        if not row:
            return {"branch_id": str(branch_id), "snapshot_date": str(snapshot_date), "total_quantity": 0, "total_stock_value": 0}

        return {
            "branch_id": str(branch_id),
            "snapshot_date": str(snapshot_date),
            "total_quantity": row.total_quantity,
            "total_stock_value": float(row.total_stock_value)
        }

analytics_service = AnalyticsService()
