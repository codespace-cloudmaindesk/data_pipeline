from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
from typing import Optional, List
from .enums import movement_type
from .enums import payment_method

class POSTransactionItem(BaseModel):
    product_id: UUID
    quantity: int
    unit_price: float
    discount: Optional[float]
    tax: Optional[float]
    total_amount: float
    movement_type: Optional[str]= movement_type.SALE

class POSTransaction(BaseModel):
    id: UUID
    branch_id: UUID
    cashier_id: Optional[UUID]
    terminal_id: Optional[str]
    items: List[POSTransactionItem]
    payment_method: Optional[str] = payment_method.CASH
    total_discount: Optional[float]
    total_tax: Optional[float]
    total_quantity: int
    total_amount: float
    timestamp: datetime
    
    model_config = {
        "from_attributes": True
    }
