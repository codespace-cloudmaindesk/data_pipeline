from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from .enums import alert_type as AlertType

class StockAlert(BaseModel):
    id: UUID
    item_id: UUID
    branch_id: UUID
    current_quantity: int
    threshold: int
    created_at: datetime
    resolved: bool = False
    alert_type: AlertType

    model_config = {
        "from_attributes": True
    }