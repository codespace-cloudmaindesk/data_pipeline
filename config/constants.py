from enum import Enum
from collections import namedtuple

Company  = namedtuple("Company",  ["code", "company_name", "country", "currency"])
Store    = namedtuple("Store",    ["code", "store_name", "brand_tier", "store_format"])
Category = namedtuple("Category", ["category_name", "division"])

class ParentCompanyEnum(Enum):
    SHOPRITE_GROUP = Company("SHPGRP", "Shoprite Group", "South Africa", "ZAR")


class ChildCompanyEnum(Enum):
    SHOPRITE         = Store("SHOP", "Shoprite", "Value", "Supermarket")
    CHECKERS         = Store("CHK", "Checkers", "Premium", "Hyper")
    USAVE            = Store("USV", "Usave", "Budget", "Discount")
    CHECKERS_SIXTY60 = Store("C60", "Checkers Sixty60", "Premium", "On-Demand")


class DivisionEnum(Enum):
    FRESH_FOOD         = "Fresh Food"
    GROCERY_ESSENTIALS = "Grocery Essentials"
    HOUSEHOLD          = "Household"
    BEVERAGES          = "Beverages"


class CategoryEnum(Enum):
    BAKERY    = Category("Bakery", DivisionEnum.FRESH_FOOD)
    DAIRY     = Category("Dairy & Eggs", DivisionEnum.FRESH_FOOD)
    PRODUCE   = Category("Produce", DivisionEnum.FRESH_FOOD)
    MEAT      = Category("Meat & Poultry", DivisionEnum.FRESH_FOOD)
    SNACKS    = Category("Snacks & Confectionery", DivisionEnum.GROCERY_ESSENTIALS)
    BEVERAGES = Category("Drinks & Beverages", DivisionEnum.BEVERAGES)
    CLEANING  = Category("Cleaning Supplies", DivisionEnum.HOUSEHOLD)


class UnitOfMeasureEnum(Enum):
    GRAMS       = "g"
    KILOGRAMS   = "kg"
    LITRES      = "L"
    MILLILITRES = "ml"
    PACK        = "Pack"
    EACH        = "Each"

class SupplierEnum(Enum):
    TIGER_BRANDS = "Tiger Brands"
    QUATUM_FOODS = "Quantum Foods"
    PREMIER_FMCG = "Premier FMCG"
    PIONEER_FOODS = "Pioneer Foods"
    PEPSICO = "PepsiCo"
    DANONE = "Danone"
    RCL_FOODS = "RCL Foods"
    WOODLANDS_DAIRY = "Woodlands Dairy"
    CLOVER = "Clover SA"
    LANCEWOOD = "Lancewood"
    PARMALAT = "Parmalat"
    UNILEVER = "Unilever"
    COCA_COLA = "Coca-Cola Beverages SA"
    RAINBOW = "Rainbow Chicken"
    ASTRAL_FOODS = "Astral Foods"
    FARMBEST = "Farmbest"
    SHOPRITE_HOUSE_BRAND = "Shoprite House Brand"
    SHOPRITE_PRIVATE_LABEL = "Shoprite Private Label"
    ZZ2 = "ZZ2"
    FRESHMARK = "Freshmark"
    MONDELEZ = "Mondelez"


PRODUCT_TEMPLATES = [
    # BAKERY
    {"brand": "Albany", "supplier": SupplierEnum.TIGER_BRANDS, "category": CategoryEnum.BAKERY, "item": "White Bread", "uom": UnitOfMeasureEnum.EACH, "pack_size": 1, "min_price": 14.0, "max_price": 18.0},
    {"brand": "Albany", "supplier": SupplierEnum.TIGER_BRANDS, "category": CategoryEnum.BAKERY, "item": "Brown Bread", "uom": UnitOfMeasureEnum.EACH, "pack_size": 1, "min_price": 13.0, "max_price": 17.0},
    {"brand": "Blue Ribbon", "supplier": SupplierEnum.PREMIER_FMCG, "category": CategoryEnum.BAKERY, "item": "White Bread", "uom": UnitOfMeasureEnum.EACH, "pack_size": 1, "min_price": 14.0, "max_price": 18.0},
    {"brand": "Sasko", "supplier": SupplierEnum.PIONEER_FOODS, "category": CategoryEnum.BAKERY, "item": "Premium White Bread", "uom": UnitOfMeasureEnum.EACH, "pack_size": 1, "min_price": 15.0, "max_price": 20.0},
    {"brand": "Sunbake", "supplier": SupplierEnum.RCL_FOODS, "category": CategoryEnum.BAKERY, "item": "Brown Bread", "uom": UnitOfMeasureEnum.EACH, "pack_size": 1, "min_price": 13.0, "max_price": 17.0},
    {"brand": "Albany", "supplier": SupplierEnum.TIGER_BRANDS, "category": CategoryEnum.BAKERY, "item": "Hot Dog Rolls", "uom": UnitOfMeasureEnum.PACK, "pack_size": 6, "min_price": 18.0, "max_price": 23.0},

    # DAIRY
    {"brand": "Clover", "supplier": SupplierEnum.CLOVER, "category": CategoryEnum.DAIRY, "item": "Full Cream Milk", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 2, "min_price": 32.0, "max_price": 38.0},
    {"brand": "Clover", "supplier": SupplierEnum.CLOVER, "category": CategoryEnum.DAIRY, "item": "Low Fat Milk", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 2, "min_price": 32.0, "max_price": 38.0},
    {"brand": "Parmalat", "supplier": SupplierEnum.PARMALAT, "category": CategoryEnum.DAIRY, "item": "Cheddar Cheese", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 400, "min_price": 50.0, "max_price": 60.0},
    {"brand": "Ladismith", "supplier": SupplierEnum.WOODLANDS_DAIRY, "category": CategoryEnum.DAIRY, "item": "Cheddar Cheese", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 400, "min_price": 48.0, "max_price": 58.0},
    {"brand": "Nulaid", "supplier": SupplierEnum.QUATUM_FOODS, "category": CategoryEnum.DAIRY, "item": "Large Eggs", "uom": UnitOfMeasureEnum.PACK, "pack_size": 18, "min_price": 45.0, "max_price": 52.0},
    {"brand": "Nulaid", "supplier": SupplierEnum.QUATUM_FOODS, "category": CategoryEnum.DAIRY, "item": "Extra Large Eggs", "uom": UnitOfMeasureEnum.PACK, "pack_size": 30, "min_price": 75.0, "max_price": 85.0},
    {"brand": "Danone", "supplier": SupplierEnum.DANONE, "category": CategoryEnum.DAIRY, "item": "NutriDay Plain Yoghurt", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 30.0, "max_price": 38.0},
    {"brand": "Crystal Valley", "supplier": SupplierEnum.LANCEWOOD, "category": CategoryEnum.DAIRY, "item": "Plain Yoghurt", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 28.0, "max_price": 36.0},

    # PRODUCE
    {"brand": "ZZ2", "supplier": SupplierEnum.ZZ2, "category": CategoryEnum.PRODUCE, "item": "Tomatoes", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 20.0, "max_price": 28.0},
    {"brand": "Nature's Choice", "supplier": SupplierEnum.FRESHMARK, "category": CategoryEnum.PRODUCE, "item": "Bananas", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 18.0, "max_price": 25.0},
    {"brand": "Freshmark", "supplier": SupplierEnum.FRESHMARK, "category": CategoryEnum.PRODUCE, "item": "Potatoes", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 2, "min_price": 30.0, "max_price": 40.0},
    {"brand": "Freshmark", "supplier": SupplierEnum.FRESHMARK, "category": CategoryEnum.PRODUCE, "item": "Onions", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 15.0, "max_price": 22.0},
    {"brand": "Freshmark", "supplier": SupplierEnum.FRESHMARK, "category": CategoryEnum.PRODUCE, "item": "Apples", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1.5, "min_price": 22.0, "max_price": 32.0},

    # MEAT
    {"brand": "Steakhouse Classic", "supplier": SupplierEnum.SHOPRITE_HOUSE_BRAND, "category": CategoryEnum.MEAT, "item": "Beef T-Bone Steak", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 110.0, "max_price": 140.0},
    {"brand": "Championship Boerewors", "supplier": SupplierEnum.SHOPRITE_PRIVATE_LABEL, "category": CategoryEnum.MEAT, "item": "Thick Boerewors", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 85.0, "max_price": 105.0},
    {"brand": "Rainbow Chicken", "supplier": SupplierEnum.RCL_FOODS, "category": CategoryEnum.MEAT, "item": "Chicken Mixed Portions (IQF)", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 2, "min_price": 85.0, "max_price": 105.0},
    {"brand": "Rainbow Chicken", "supplier": SupplierEnum.RCL_FOODS, "category": CategoryEnum.MEAT, "item": "Fresh Whole Chicken", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1.5, "min_price": 70.0, "max_price": 90.0},
    {"brand": "Country Fair", "supplier": SupplierEnum.ASTRAL_FOODS, "category": CategoryEnum.MEAT, "item": "Chicken Braaipack", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1.5, "min_price": 75.0, "max_price": 95.0},
    {"brand": "Farmbest", "supplier": SupplierEnum.FARMBEST, "category": CategoryEnum.MEAT, "item": "Pork Chops", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 1, "min_price": 80.0, "max_price": 100.0},

    # SNACKS
    {"brand": "Simba", "supplier": SupplierEnum.PEPSICO, "category": CategoryEnum.SNACKS, "item": "Smoked Beef Chips", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 120, "min_price": 16.0, "max_price": 20.0},
    {"brand": "Simba", "supplier": SupplierEnum.PEPSICO, "category": CategoryEnum.SNACKS, "item": "Cheese & Onion Chips", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 120, "min_price": 16.0, "max_price": 20.0},
    {"brand": "NikNaks", "supplier": SupplierEnum.PEPSICO, "category": CategoryEnum.SNACKS, "item": "Cheese Snacks", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 135, "min_price": 14.0, "max_price": 18.0},
    {"brand": "Cadbury", "supplier": SupplierEnum.MONDELEZ, "category": CategoryEnum.SNACKS, "item": "Dairy Milk Chocolate Slab", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 150, "min_price": 26.0, "max_price": 32.0},
    {"brand": "Beacon", "supplier": SupplierEnum.TIGER_BRANDS, "category": CategoryEnum.SNACKS, "item": "Sweets Pack", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 100, "min_price": 15.0, "max_price": 20.0},
    {"brand": "Doritos", "supplier": SupplierEnum.PEPSICO, "category": CategoryEnum.SNACKS, "item": "Sweet Chilli Pepper Chips", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 145, "min_price": 18.0, "max_price": 23.0},
    {"brand": "Lays", "supplier": SupplierEnum.PEPSICO, "category": CategoryEnum.SNACKS, "item": "Lightly Salted Chips", "uom": UnitOfMeasureEnum.GRAMS, "pack_size": 120, "min_price": 18.0, "max_price": 22.0},

    # BEVERAGES
    {"brand": "Coca-Cola", "supplier": SupplierEnum.COCA_COLA, "category": CategoryEnum.BEVERAGES, "item": "Coca-Cola Original", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 2, "min_price": 21.0, "max_price": 26.0},
    {"brand": "Coca-Cola", "supplier": SupplierEnum.COCA_COLA, "category": CategoryEnum.BEVERAGES, "item": "Coca-Cola No Sugar", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 2, "min_price": 21.0, "max_price": 26.0},
    {"brand": "Oros", "supplier": SupplierEnum.TIGER_BRANDS, "category": CategoryEnum.BEVERAGES, "item": "Orange Squash", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 2, "min_price": 38.0, "max_price": 45.0},
    {"brand": "Liqui Fruit", "supplier": SupplierEnum.PIONEER_FOODS, "category": CategoryEnum.BEVERAGES, "item": "100% Fruit Juice", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 1, "min_price": 22.0, "max_price": 28.0},
    {"brand": "Cappy", "supplier": SupplierEnum.COCA_COLA, "category": CategoryEnum.BEVERAGES, "item": "Orange Juice Blend", "uom": UnitOfMeasureEnum.LITRES, "pack_size": 1.5, "min_price": 25.0, "max_price": 32.0},

    # CLEANING
    {"brand": "Sunlight", "supplier": SupplierEnum.UNILEVER, "category": CategoryEnum.CLEANING, "item": "Dishwashing Liquid", "uom": UnitOfMeasureEnum.MILLILITRES, "pack_size": 750, "min_price": 29.0, "max_price": 36.0},
    {"brand": "Omo", "supplier": SupplierEnum.UNILEVER, "category": CategoryEnum.CLEANING, "item": "Auto Washing Powder", "uom": UnitOfMeasureEnum.KILOGRAMS, "pack_size": 2, "min_price": 85.0, "max_price": 105.0},
    {"brand": "Handy Andy", "supplier": SupplierEnum.UNILEVER, "category": CategoryEnum.CLEANING, "item": "Ammonia Floor Cleaner", "uom": UnitOfMeasureEnum.MILLILITRES, "pack_size": 750, "min_price": 26.0, "max_price": 33.0},
    {"brand": "Domestos", "supplier": SupplierEnum.UNILEVER, "category": CategoryEnum.CLEANING, "item": "Thick Bleach", "uom": UnitOfMeasureEnum.MILLILITRES, "pack_size": 750, "min_price": 32.0, "max_price": 38.0},
]

class PriceTypeEnum(Enum):
    REGULAR = "Regular"
    PROMO = "Promo"


class SourceSystemEnum(Enum):
    SAP_ERP = "SAP_ERP"
    POS = "POS"
    PRICING_ENGINE = "Shoprite X"
    CRM = "CRM"
    LOYALTY = "LoyaltySystem"
    ONLINE = "OnlinePlatform"

class PaymentMethodEnum(Enum):
    CASH = "Cash"
    CARD = "Card"
    MOBILE = "Mobile Wallet"
    EFT = "EFT"
    VOUCHER = "Voucher"


class CustomerTypeEnum(Enum):
    RETAIL    = "Retail"
    BULK      = "Bulk"
    WHOLESALE = "Wholesale"


class ChannelEnum(Enum):
    IN_STORE = "In-Store"
    ONLINE   = "Online"


CITY_TO_PROVINCE = {
    "Johannesburg": "Gauteng",
    "Pretoria": "Gauteng",
    "Tembisa": "Gauteng",
    "Soweto": "Gauteng",

    "Cape Town": "Western Cape",
    "Stellenbosch": "Western Cape",

    "Durban": "KwaZulu-Natal",
    "Pietermaritzburg": "KwaZulu-Natal",

    "Bloemfontein": "Free State",
    "Polokwane": "Limpopo",
    "Mbombela": "Mpumalanga",
    "Rustenburg": "North West",
    "Kimberley": "Northern Cape",
}

FISCAL_YEARS = [2024, 2025]
