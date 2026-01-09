from enum import Enum

class InventoryMovementType(str, Enum):
    IN = "IN"
    OUT = "OUT"
    ADJUSTMENT = "ADJUSTMENT"
    RETURN = "RETURN"
    TRANSFER = "TRANSFER"
    SALE = "SALE"
    PURCHASE = "PURCHASE"

class ProductCategory(str, Enum):
    GROCERY = "GROCERY"
    PRODUCE = "PRODUCE"
    MEAT = "MEAT"
    BAKERY = "BAKERY"
    HOUSEHOLD = "HOUSEHOLD"

class AlertType(str, Enum):
    LOW_STOCK = "LOW_STOCK"
    HIGH_STOCK = "HIGH_STOCK"