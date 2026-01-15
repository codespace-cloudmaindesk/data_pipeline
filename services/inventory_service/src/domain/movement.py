from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from typing import Optional
from .enums import movement_type

class InventoryMovement(BaseModel):
    id: UUID
    transaction_id: UUID
    item_id: UUID
    branch_id: UUID
    movement_type: movement_type
    quantity: int
    timestamp: datetime
    reason: Optional[str]

    model_config = {
        "from_attributes": True
    }