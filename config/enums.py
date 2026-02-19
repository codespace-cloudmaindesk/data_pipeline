from enum import Enum


class PlatformEnum(str, Enum):
    ECOMMERCE = "E-Commerce"
    IN_STORE = "In-Store"


class ChannelEnum(str, Enum):
    DIRECT = "Direct"
    RETAILER = "Retailer"
    REFERRAL = "Referral"


class RegionEnum(str, Enum):
    NORTH = "North"
    SOUTH = "South"
    EAST = "East"
    WEST = "West"
    CENTRAL = "Central"


class DivisionEnum(str, Enum):
    FRESH_FOOD = "Fresh Food"
    GROCERY = "Grocery Essentials"
    HOUSEHOLD = "Household & Cleaning"


class CategoryEnum(str, Enum):
    BAKERY = "Bakery"
    DAIRY = "Dairy & Eggs"
    PRODUCE = "Fruits & Vegetables"
    MEAT = "Meat & Poultry"
    SNACKS = "Snacks & Confectionery"
    CLEANING = "Cleaning Supplies"


CATEGORY_TO_DIVISION: dict[CategoryEnum, DivisionEnum] = {
    CategoryEnum.BAKERY: DivisionEnum.FRESH_FOOD,
    CategoryEnum.DAIRY: DivisionEnum.FRESH_FOOD,
    CategoryEnum.PRODUCE: DivisionEnum.FRESH_FOOD,
    CategoryEnum.MEAT: DivisionEnum.FRESH_FOOD,
    CategoryEnum.SNACKS: DivisionEnum.GROCERY,
    CategoryEnum.CLEANING: DivisionEnum.HOUSEHOLD,
}


PRODUCT_NAMES: dict[CategoryEnum, list[str]] = {
    CategoryEnum.BAKERY: [
        "Sourdough Loaf", "Whole Wheat Bread", "Croissant", "Baguette",
        "Ciabatta Roll", "Rye Bread", "Brioche Bun", "Flatbread",
        "Muffin Pack", "Scone Selection",
    ],
    CategoryEnum.DAIRY: [
        "Free Range Eggs", "Full Cream Milk", "Greek Yoghurt",
        "Cheddar Cheese", "Butter Block", "Cottage Cheese",
        "Gouda Slices", "Cream Cheese Tub", "Amasi", "Maas",
    ],
    CategoryEnum.PRODUCE: [
        "Banana Bunch", "Baby Spinach", "Tomatoes on Vine",
        "Butternut Squash", "Avocado Pack", "Red Onions",
        "Mixed Peppers", "Sweet Potatoes", "Gem Squash", "Broccoli Head",
    ],
    CategoryEnum.MEAT: [
        "Chicken Breast Fillet", "Beef Mince", "Lamb Chops",
        "Pork Sausages", "Boerewors Roll", "Biltong Sliced",
        "Chicken Drumsticks", "Stewing Beef", "Turkey Rashers", "Droëwors",
    ],
    CategoryEnum.SNACKS: [
        "Potato Crisps", "Dark Chocolate Bar", "Trail Mix",
        "Rice Cakes", "Peanut Butter Pretzels", "Biscuit Assortment",
        "Dried Mango Strips", "Popcorn Multipack", "Energy Bar", "Rusks",
    ],
    CategoryEnum.CLEANING: [
        "All-Purpose Cleaner", "Dishwashing Liquid", "Laundry Detergent",
        "Bleach Concentrate", "Glass Cleaner Spray", "Floor Polish",
        "Fabric Softener", "Toilet Cleaner Gel", "Surface Wipes", "Bin Liners",
    ],
}


class UnitOfMeasureEnum(str, Enum):
    GRAMS = "g"
    KILOGRAMS = "kg"
    LITRES = "L"
    MILLILITRES = "ml"
    PACKS = "Pack"
    PIECES = "Each"