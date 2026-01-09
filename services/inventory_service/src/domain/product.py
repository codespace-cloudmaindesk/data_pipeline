from pydantic import BaseModel
from uuid import UUID
from .enums import ProductCategory

class Product(BaseModel):
    id: UUID
    sku: str
    name: str
    category: ProductCategory

    class Config:
        orm_mode = True
