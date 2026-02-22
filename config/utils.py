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

PRODUCT_REGISTRY = {
    CategoryEnum.BAKERY: {
        "brands": ["Albany", "Blue Ribbon", "Sasko", "Sunbake"],
        "items": ["White Bread", "Brown Bread", "Seeded Brown Bread", "Hot Dog Rolls (6pk)"],
        "variants": [UnitOfMeasureEnum.EACH, UnitOfMeasureEnum.GRAMS, UnitOfMeasureEnum.PACK]
    },

    CategoryEnum.DAIRY: {
        "brands": ["Clover", "Parmalat", "Ladismith", "Nulaid", "Danone", "Crystal Valley"],
        "items": ["Full Cream Milk", "Low Fat Milk", "Plain Yoghurt", "Cheddar Cheese", "Margarine", "Large Eggs (18)"],
        "variants": [UnitOfMeasureEnum.LITRES, UnitOfMeasureEnum.MILLILITRES, UnitOfMeasureEnum.GRAMS, UnitOfMeasureEnum.EACH]
    },

    CategoryEnum.PRODUCE: {
        "brands": ["ZZ2", "Nature's Choice", "Freshmark"],
        "items": ["Bananas", "Potatoes", "Tomatoes", "Onions", "Apples"],
        "variants": [UnitOfMeasureEnum.KILOGRAMS, UnitOfMeasureEnum.GRAMS, UnitOfMeasureEnum.EACH]
    },

    CategoryEnum.MEAT: {
        "brands": ["Karan Beef", "Rainbow Chicken", "Country Fair"],
        "items": ["Beef Stewing", "Chicken Portions", "Boerewors", "Pork Chops"],
        "variants": [UnitOfMeasureEnum.KILOGRAMS, UnitOfMeasureEnum.GRAMS]
    },

    CategoryEnum.SNACKS: {
        "brands": ["Simba", "NikNaks", "Cadbury", "Beacon", "Doritos", "Lays"],
        "items": ["Potato Chips", "Cheese Snacks", "Chocolate Slab", "Salted Chips", "Sweets Pack", "Chips"],
        "variants": [UnitOfMeasureEnum.GRAMS, UnitOfMeasureEnum.PACK, UnitOfMeasureEnum.EACH]
    },

    CategoryEnum.BEVERAGES: {
        "brands": ["Coca-Cola", "Oros", "Liqui Fruit", "Cappy"],
        "items": ["Cola", "Orange Squash", "100% Fruit Juice", "Flavoured Drink"],
        "variants": [UnitOfMeasureEnum.LITRES, UnitOfMeasureEnum.MILLILITRES, UnitOfMeasureEnum.PACK]
    },

    CategoryEnum.CLEANING: {
        "brands": ["Sunlight", "Omo", "Handy Andy", "Domestos"],
        "items": ["Dishwashing Liquid", "Washing Powder", "Floor Cleaner", "Bleach"],
        "variants": [UnitOfMeasureEnum.LITRES, UnitOfMeasureEnum.MILLILITRES, UnitOfMeasureEnum.PACK]
    }
}

PRICE_RULES = {
    CategoryEnum.BAKERY: {
        #--------(min, max) price--------
        UnitOfMeasureEnum.GRAMS: (8, 35),
        UnitOfMeasureEnum.KILOGRAMS: (18, 45),
        UnitOfMeasureEnum.EACH: (6, 20),
    },
    CategoryEnum.BEVERAGES: {
    UnitOfMeasureEnum.MILLILITRES: (8, 25),
    UnitOfMeasureEnum.LITRES: (15, 45),
    UnitOfMeasureEnum.PACK: (25, 90),
    },
    CategoryEnum.DAIRY: {
    UnitOfMeasureEnum.MILLILITRES: (10, 35),
    UnitOfMeasureEnum.LITRES: (18, 45),
    UnitOfMeasureEnum.EACH: (12, 30),
    },
    CategoryEnum.MEAT: {
    UnitOfMeasureEnum.KILOGRAMS: (75, 220),
    },
    CategoryEnum.PRODUCE: {
    UnitOfMeasureEnum.KILOGRAMS: (10, 45),
    UnitOfMeasureEnum.EACH: (3, 15),
    },
    CategoryEnum.CLEANING: {
    UnitOfMeasureEnum.MILLILITRES: (20, 80),
    UnitOfMeasureEnum.LITRES: (35, 120),
    UnitOfMeasureEnum.PACK: (25, 90),
    },
    CategoryEnum.SNACKS: {
    UnitOfMeasureEnum.EACH: (5, 25),
    }
}

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
