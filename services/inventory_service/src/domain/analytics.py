from pydantic import BaseModel
from uuid import UUID
from datetime import date

class StockMetrics(BaseModel):
    item_id: UUID
    branch_id: UUID
    total_in: int
    total_out: int
    current_stock: int

    model_config = {
        "from_attributes": True
    }

class StockTrend(BaseModel):
    item_id: UUID
    branch_id: UUID
    day: date
    quantity: int

    model_config = {
        "from_attributes": True
    }

class InventorySnapshot(BaseModel):
    snapshot_date: date
    item_id: UUID
    branch_id: UUID
    quantity: int
    unit_price: float
    stock_value: float
    total_value: float
    out_of_stock: bool

    model_config = {
        "from_attributes": True
    }
