from pydantic import BaseModel
from uuid import UUID
from typing import Optional

class ReorderSuggestion(BaseModel):
    item_id: UUID
    branch_id: UUID
    current_stock: int
    threshold: int
    suggested_quantity: int
    supplier_id: Optional[UUID]

    model_config = {
        "from_attributes": True
    }
