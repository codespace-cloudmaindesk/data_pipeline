from faker import Faker
from datetime import date
import os
import csv
import random
import uuid
from faker.providers import BaseProvider
from utils import (
    CustomerTypeEnum,
    ChannelEnum,
    CategoryEnum,
    DivisionEnum,
    PRODUCT_REGISTRY,
    CITY_TO_PROVINCE,
    FISCAL_YEARS,
    PRICE_RULES,
    UnitOfMeasureEnum,
    ChildCompanyEnum,
)



fake = Faker('zu_ZA')
#-------Helper functions--------

def _product_code():
    return f"SKU-{fake.random_number(digits=6)}"

def _product_name(category: CategoryEnum, brand: str):
    return f"{fake.random_element(PRODUCT_REGISTRY[category]['items'])} {brand}"

def _customer_code():
    return f"CUST-{fake.random_number(digits=6)}"


def generate_dim_product(row_count: int) -> list[dict]:
    """Generate product dimension rows with realistic names."""
    products: list[dict] = []

    for _ in range(row_count):
        category_enum = fake.random_element(list(CategoryEnum))
        division_enum = category_enum.value.division
        brand = fake.random_element(PRODUCT_REGISTRY[category_enum]["brands"])
        uom_enum = fake.random_element(PRODUCT_REGISTRY[category_enum]["variants"])

        products.append({
            "product_code": _product_code(),

            "product": _product_name(category_enum, brand),
            "division": division_enum.value,
            "category": category_enum.value.category_name,
            "brand": brand,
            "variant": uom_enum.value,

            "category_enum": category_enum,
            "uom_enum": uom_enum,
        })

    return products
def generate_dim_customer(row_count: int) -> list[dict]:
    customers: list[dict] = []

    for _ in range(row_count):
        city = fake.random_element(list(CITY_TO_PROVINCE.keys()))
        store = random.choice(list(ChildCompanyEnum)).value
        customers.append({
            "customer_code": _customer_code(),
            "customer_name": fake.name(),
            "store_name": store.store_name,
            "customer_type": fake.random_element(list(CustomerTypeEnum)).value,
            "channel": fake.random_element(list(ChannelEnum)).value,
            "province": CITY_TO_PROVINCE[city],
            "city": city,
        })

    return customers

def generate_dim_gross_price(products: list[dict]) -> list[dict]:
    """Generate a price row per product per fiscal year."""
    gross_prices: list[dict] = []
    BASE_INFLATION = 0.06

    for product in products:
        category = product["category_enum"]
        uom = product["uom_enum"]

        if category not in PRICE_RULES or uom not in PRICE_RULES[category]:
            continue

        min_price, max_price = PRICE_RULES[category][uom]
        base_price = random.uniform(min_price, max_price)

        for year_index, year in enumerate(FISCAL_YEARS):
            price = round(base_price * ((1 + BASE_INFLATION) ** year_index) - 0.01, 2)

            gross_prices.append({
                "product_code": product["product_code"],
                "price_zar": price,
                "currency": "ZAR",
                "year": year,
            })

    return gross_prices

def generate_fact_orders(row_count: int, customers: list[dict], products: list[dict]) -> list[dict]:
    """Generate fact order rows referencing valid product and customer codes."""
    fact_orders: list[dict] = []
    
    valid_products = [
        p for p in products 
        if p["category_enum"] in PRICE_RULES and p["uom_enum"] in PRICE_RULES[p["category_enum"]]
    ]
    
    if not valid_products or not customers:
        return fact_orders

    for _ in range(row_count):
        customer = random.choice(customers)
        product = random.choice(valid_products)
        
        category = product["category_enum"]
        uom = product["uom_enum"]
        min_price, max_price = PRICE_RULES[category][uom]
        price_zar = round(random.uniform(min_price, max_price), 2)
        
        fact_orders.append({
            "order_date": fake.date_between(start_date=date(FISCAL_YEARS[0], 1, 1), end_date=date(FISCAL_YEARS[-1], 12, 31)),
            "customer_code": customer["customer_code"],
            "product_code": product["product_code"],
            "sold_quantity": fake.random_int(min=10, max=999),
            "gross_amount": price_zar
        })
    return fact_orders

def write_to_csv(data: list[dict], path: str, filename: str, fieldnames: list[str] | None = None) -> None:
    """Write a list of dicts to a CSV file, creating directories as needed."""
    if not data:
        return
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    with open(filepath, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames or data[0].keys(), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    products = generate_dim_product(795)
    customers = generate_dim_customer(250)
    gross_prices = generate_dim_gross_price(products)
    fact_orders = generate_fact_orders(93065, customers, products)
    
    PRODUCT_FIELDS = ["product_code", "product", "division", "category", "brand", "variant"]
    CUSTOMER_FIELDS = ["customer_code", "customer_name", "store_name", "customer_type", "channel", "province", "city"]
    
    write_to_csv(gross_prices, "../data/raw", "dim_gross_price.csv")
    write_to_csv(products, "../data/raw", "dim_product.csv", fieldnames=PRODUCT_FIELDS)
    write_to_csv(customers, "../data/raw", "dim_customer.csv", fieldnames=CUSTOMER_FIELDS)
    write_to_csv(fact_orders, "../data/raw", "fact_orders.csv")

    print(f"Successfully generated {len(gross_prices)} rows of dim_gross_price.csv")
    print(f"Successfully generated {len(products)} rows of dim_product.csv")
    print(f"Successfully generated {len(customers)} rows of dim_customer.csv")
    print(f"Successfully generated {len(fact_orders)} rows of fact_orders.csv")
