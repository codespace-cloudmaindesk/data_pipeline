from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from uuid import UUID
from .enums import category, department

class InventoryItem(BaseModel):
    id: UUID
    product_id: UUID
    branch_id: UUID
    name: str
    sku: str
    category: category
    department: department
    quantity: int
    unit_price: float
    threshold: Optional[int] = 10
    last_updated: Optional[datetime]

    model_config = {
        "from_attributes": True
    }