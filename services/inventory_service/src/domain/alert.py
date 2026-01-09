from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class StockAlert(BaseModel):
    id: UUID
    item_id: UUID
    branch_id: UUID
    current_quantity: int
    threshold: int
    created_at: datetime
    resolved: bool = False
    alert_type: AlertType

    class Config:
        orm_mode = True