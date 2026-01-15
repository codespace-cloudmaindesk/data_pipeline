from enum import Enum
from typing import Dict, List

class movement_type(str, Enum):
    IN = "IN"
    OUT = "OUT"
    ADJUSTMENT = "ADJUSTMENT"
    RETURN = "RETURN"
    TRANSFER = "TRANSFER"
    SALE = "SALE"
    PURCHASE = "PURCHASE"

class department(str, Enum):
    GROCERY = "Grocery"
    FRESH_FOOD = "Fresh Food"
    HOUSEHOLD = "Household"
    LIQUOR = "Liquor & Wine"
    HEALTH_BEAUTY = "Health & Beauty"

class category(str, Enum):
    # Grocery Sub-types
    PANTRY = "Pantry"
    BEVERAGES = "Beverages"
    CANNED_GOODS = "Canned Goods"
    
    # Fresh Food Sub-types
    FRUIT_VEG = "Fruit & Vegetables"
    BUTCHERY = "Butchery"
    BAKERY = "Bakery"
    
    # Household Sub-types
    CLEANING = "Cleaning Supplies"
    PET_CARE = "Pet Care"

    # Liquor Sub-types
    SPIRITS = "Spirits"
    BEER = "Beer"
    WINE = "Wine"
    
    # Health & Beauty Sub-types
    SKINCARE = "Skin Care"
    HAIRCARE = "Hair Care"

# Mapping of departments to categories
DEPARTMENT_MAPPING: Dict[department, List[category]] = {
    department.GROCERY: [category.PANTRY, category.BEVERAGES, category.CANNED_GOODS],
    department.FRESH_FOOD: [category.FRUIT_VEG, category.BUTCHERY, category.BAKERY],
    department.HOUSEHOLD: [category.CLEANING, category.PET_CARE],
    department.LIQUOR: [category.SPIRITS, category.BEER, category.WINE],
    department.HEALTH_BEAUTY: [category.SKINCARE, category.HAIRCARE]
}

class alert_type(str, Enum):
    LOW_STOCK = "LOW_STOCK"
    HIGH_STOCK = "HIGH_STOCK"

class branch_code(str, Enum):
    BR_01 = "550e8400-e29b-41d4-a716-446655440001"
    BR_02 = "550e8400-e29b-41d4-a716-446655440002"
    BR_03 = "550e8400-e29b-41d4-a716-446655440003"
    BR_04 = "550e8400-e29b-41d4-a716-446655440004"

class payment_method(str, Enum):
    CASH = "Cash"
    CARD = "Card"
    EFT = "EFT"  
    MOBILE = "Mobile Payment"  
    VOUCHER = "Voucher"
    OTHER = "Other"
    