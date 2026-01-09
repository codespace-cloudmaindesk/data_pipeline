from pydantic import BaseModel
from uuid import UUID
from datetime import date

class StockMetrics(BaseModel):
    item_id: UUID
    branch_id: UUID
    total_in: int
    total_out: int
    current_stock: int

    class Config:
        orm_mode = True

class StockTrend(BaseModel):
    item_id: UUID
    branch_id: UUID
    day: date
    quantity: int

    class Config:
        orm_mode = True
