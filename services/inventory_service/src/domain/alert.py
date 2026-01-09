from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class LowStockAlert(BaseModel):
    id: UUID
    item_id: UUID
    branch_id: UUID
    current_quantity: int
    threshold: int
    created_at: datetime
    resolved: bool = False

    class Config:
        orm_mode = True