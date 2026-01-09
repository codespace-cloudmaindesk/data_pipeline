from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from typing import Optional
from .enums import InventoryMovementType

class InventoryMovement(BaseModel):
    id: UUID
    transaction_id: UUID
    item_id: UUID
    branch_id: UUID
    movement_type: InventoryMovementType
    quantity: int
    timestamp: datetime
    reason: Optional[str]

    class Config:
        orm_mode = True