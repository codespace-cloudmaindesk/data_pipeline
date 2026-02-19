import csv
import os
import random
from dotenv import dotenv_values
from faker import Faker
from enums import (
    PlatformEnum, ChannelEnum, RegionEnum,
    CategoryEnum, UnitOfMeasureEnum,
    CATEGORY_TO_DIVISION, PRODUCT_NAMES,
)

_cfg = dotenv_values(os.path.join(os.path.dirname(__file__), "../.env"))

PRODUCT_ROW_COUNT   = int(_cfg["DIM_PRODUCT_ROW_COUNT"])
CUSTOMER_ROW_COUNT  = int(_cfg["DIM_CUSTOMER_TARGET_ROW_COUNT"])
ORDER_ROW_COUNT     = int(_cfg["FACT_ORDER_TARGET_ROW_COUNT"])
RANDOM_SEED         = int(_cfg["RANDOM_SEED"])
FAKER_LOCALE        = _cfg["FAKER_LOCALE"]
CODE_PAD_LEN        = int(_cfg["CODE_PADDING_LENGTH"])
FISCAL_YEARS        = [int(y) for y in _cfg["FISCAL_YEARS"].split(",")]
BASE_OUTPUT_PATH    = _cfg["BASE_OUTPUT_PATH"]

PLATFORM_WEIGHTS = tuple(float(w) for w in _cfg["PLATFORM_DISTRIBUTION_WEIGHTS"].split(","))
CHANNEL_WEIGHTS  = tuple(float(w) for w in _cfg["CHANNEL_DISTRIBUTION_WEIGHTS"].split(","))
REGION_WEIGHTS   = tuple(float(w) for w in _cfg["REGION_DISTRIBUTION_WEIGHTS"].split(","))

fake = Faker([FAKER_LOCALE])
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

def _weighted_choice(enum_cls, weights: tuple[float, ...]) -> str:
    """Pick a random enum value using the supplied probability weights."""
    return random.choices([e.value for e in enum_cls], weights=weights, k=1)[0]


def _product_code() -> str:
    return f"PROD{fake.unique.random_number(CODE_PAD_LEN)}SA"


def _customer_code() -> str:
    return f"CUST{fake.unique.random_number(CODE_PAD_LEN)}SA"

def generate_dim_product(row_count: int) -> list[dict]:
    """Generate product dimension rows with realistic names."""
    products: list[dict] = []
    for _ in range(row_count):
        category = fake.random_element(list(CategoryEnum))
        division = CATEGORY_TO_DIVISION[category]
        product_name = fake.random_element(PRODUCT_NAMES[category])

        products.append({
            "product_code": _product_code(),
            "division": division.value,
            "category": category.value,
            "product": product_name,
            "variant": fake.random_element([v.value for v in UnitOfMeasureEnum]),
        })
    return products


def generate_dim_customer(row_count: int) -> list[dict]:
    """Generate customer dimension rows using weighted distributions."""
    customers: list[dict] = []
    for _ in range(row_count):
        customers.append({
            "customer_code": _customer_code(),
            "customer_name": fake.name(),
            "platform": _weighted_choice(PlatformEnum, PLATFORM_WEIGHTS),
            "channel": _weighted_choice(ChannelEnum, CHANNEL_WEIGHTS),
            "region": _weighted_choice(RegionEnum, REGION_WEIGHTS),
        })
    return customers


def generate_dim_gross_price(product_codes: list[str]) -> list[dict]:
    """Generate a price row per product per fiscal year."""
    gross_prices: list[dict] = []
    for code in product_codes:
        for year in FISCAL_YEARS:
            gross_prices.append({
                "product_code": code,
                "price_zar": fake.random_int(min=15, max=1999),
                "year": year,
            })
    return gross_prices


def generate_fact_orders(
    row_count: int,
    product_codes: list[str],
    customer_codes: list[str],
) -> list[dict]:
    """Generate fact order rows referencing valid product and customer codes."""
    fact_orders: list[dict] = []
    for _ in range(row_count):
        fact_orders.append({
            "date": fake.date_this_year(),
            "customer_code": random.choice(customer_codes),
            "product_code": random.choice(product_codes),
            "sold_quantity": fake.random_int(min=1, max=30),
        })
    return fact_orders

def write_to_csv(data: list[dict], path: str, filename: str) -> None:
    """Write a list of dicts to a CSV file, creating directories as needed."""
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    with open(filepath, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    products  = generate_dim_product(PRODUCT_ROW_COUNT)
    customers = generate_dim_customer(CUSTOMER_ROW_COUNT)

    product_codes  = [p["product_code"] for p in products]
    customer_codes = [c["customer_code"] for c in customers]

    gross_prices = generate_dim_gross_price(product_codes)
    orders       = generate_fact_orders(ORDER_ROW_COUNT, product_codes, customer_codes)

    write_to_csv(products,     BASE_OUTPUT_PATH, "dim_product.csv")
    write_to_csv(customers,    BASE_OUTPUT_PATH, "dim_customer.csv")
    write_to_csv(gross_prices, BASE_OUTPUT_PATH, "dim_gross_price.csv")
    write_to_csv(orders,       BASE_OUTPUT_PATH, "fact_orders.csv")

    print(f"[INFO] Files generated successfully at {BASE_OUTPUT_PATH}")
    print(f"  • dim_product.csv      : {len(products):,} rows")
    print(f"  • dim_customer.csv     : {len(customers):,} rows")
    print(f"  • dim_gross_price.csv  : {len(gross_prices):,} rows")
    print(f"  • fact_orders.csv      : {len(orders):,} rows")