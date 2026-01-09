from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from typing import Optional

class POSTransaction(BaseModel):
    id: UUID
    branch_id: UUID
    cashier_id: Optional[UUID]
    terminal_id: Optional[str]
    timestamp: datetime

    class Config:
        orm_mode = True
