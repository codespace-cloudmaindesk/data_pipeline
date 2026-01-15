from pydantic import BaseModel
from uuid import UUID
from typing import Optional
from .enums import category, department

class Product(BaseModel):
    id: UUID
    sku: str
    name: str
    category: category
    department: department
    brand: Optional[str]
    price: float
    is_promo: bool
    promo_price: Optional[float]

    model_config = {
        "from_attributes": True
    }
