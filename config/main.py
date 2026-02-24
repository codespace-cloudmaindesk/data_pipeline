from faker import Faker
from datetime import date, datetime, timedelta
import calendar
import os
import csv
import random
from dotenv import load_dotenv, find_dotenv
from faker.providers import BaseProvider
from constants import (
    CustomerTypeEnum,
    ChannelEnum,
    CategoryEnum,
    DivisionEnum,
    PRODUCT_TEMPLATES,
    CITY_TO_PROVINCE,
    FISCAL_YEARS,
    PriceTypeEnum,
    SupplierEnum,
    SourceSystemEnum,
    PaymentMethodEnum,
    UnitOfMeasureEnum,  
    ChildCompanyEnum,
)

def inject_data_issues(data: list[dict], nullable_fields: list[str], duplicate_prob: float = 0.02, null_prob: float = 0.03) -> list[dict]:
    """Injects realistic data issues like nulls and duplicates into the dataset."""
    result = []
    
    for row in data:
        # 1. Null Injection
        if random.random() < null_prob and nullable_fields:
            field_to_null = random.choice(nullable_fields)
            row[field_to_null] = None
        
        result.append(row)
        
        # 2. Duplicate Injection
        if random.random() < duplicate_prob:
            # Create a shallow copy and potentially alter batch_id or timestamp to simulate a re-run or replay
            duplicate_row = row.copy()
            if random.random() < 0.5:
                # 50% chance the duplicate arrived in a different batch/time
                duplicate_row["load_timestamp"] = current_load_timestamp()
            result.append(duplicate_row)
            
    return result



load_dotenv(find_dotenv())
fake = Faker(os.getenv("FAKER_LOCALE", "zu_ZA"))

#-------Helper functions--------

_issued_product_codes: set[str] = set()
_issued_customer_codes: set[str] = set()
_issued_order_codes: set[str] = set()

def generate_batch_id():
    return f"BATCH-{datetime.now().strftime('%Y%m%d')}-01"

def current_load_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _generate_unique_code(prefix: str, issued_codes: set) -> str:
    while True:
        code = f"{prefix}-{fake.random_number(digits=6, fix_len=True):06d}"
        if code not in issued_codes:
            issued_codes.add(code)
            return code

def _product_code():
    return _generate_unique_code("SKU", _issued_product_codes)

def _customer_code():
    return _generate_unique_code("CUST", _issued_customer_codes)

def _order_id():
    return _generate_unique_code("ORD", _issued_order_codes)

def _generate_order_dates(row_count: int, chunk_size: int) -> list:
    """Generate a randomized list of dates grouped into chunks."""
    dates = []
    remaining = row_count
    
    while remaining > 0:
        current_chunk = chunk_size or random.randint(3, 10)
        
        # Prevent small leftover trailing sizes if chunk_size is arbitrary
        if not chunk_size and 0 < remaining - current_chunk < 3:
            current_chunk = remaining
            
        current_chunk = min(current_chunk, remaining)
        
        chunk_date = fake.date_between(
            start_date=date(FISCAL_YEARS[0], 1, 1), 
            end_date=date(FISCAL_YEARS[-1], 12, 31)
        )
        dates.extend([chunk_date] * current_chunk)
        remaining -= current_chunk
        
    random.shuffle(dates)
    return dates

def generate_product(row_count: int) -> list[dict]:
    """Generate product rows with realistic names."""
    products: list[dict] = []
    batch_id = generate_batch_id()
    load_timestamp = current_load_timestamp()

    for _ in range(row_count):
        template = random.choice(PRODUCT_TEMPLATES)
        
        category_enum = template["category"]
        division_enum = category_enum.value.division
        brand = template["brand"]
        uom_enum = template["uom"]
        pack_size = template["pack_size"]
        
        min_price = template["min_price"]
        max_price = template["max_price"]
        standard_cost = round(random.uniform(min_price, max_price), 2)

        products.append({
            "product_sku": _product_code(),
            "product_name": f"{brand} {template['item']}",
            "brand": brand,
            "division": division_enum.value,
            "category": category_enum.value.category_name,
            "unit_of_measure(varients)": uom_enum.value,
            "pack_size": pack_size,
            "standard_cost": standard_cost,
            "supplier_name": template["supplier"].value,
            "source_system": SourceSystemEnum.SAP_ERP.value,
            "load_timestamp": load_timestamp,
            "batch_id": batch_id,
        })

    return products

def generate_gross_price(products: list[dict]) -> list[dict]:
    """Generate gross price rows with realistic prices."""
    gross_prices: list[dict] = []
    BASE_INFLATION = 0.06
    batch_id = generate_batch_id()
    load_timestamp = current_load_timestamp()

    for product in products:
        base_price = product["standard_cost"]
        for year_index, year in enumerate(FISCAL_YEARS):
            price = round(base_price * ((1 + BASE_INFLATION) ** year_index) - 0.01, 2)
            
            price_type = random.choice(list(PriceTypeEnum)).value
            
            month = random.randint(1, 12)
            day = random.randint(1, 28) 
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            valid_from_dt = datetime(year, month, day, hour, minute, second)
            
            if price_type == PriceTypeEnum.REGULAR.value:
                days_in_month = calendar.monthrange(valid_from_dt.year, valid_from_dt.month)[1]
                valid_to_dt = valid_from_dt + timedelta(days=days_in_month)
            else: # PROMO
                valid_to_dt = valid_from_dt + timedelta(days=7)

            gross_prices.append({
                "product_sku": product["product_sku"],
                "store_code": random.choice(list(ChildCompanyEnum)).value.code,

                "gross_price": price,
                "currency": "ZAR",
                "price_type": price_type,
                "valid_from": valid_from_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "valid_to": valid_to_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "source_system": SourceSystemEnum.PRICING_ENGINE.value,
                "load_timestamp": load_timestamp,
                "batch_id": batch_id,
            })

    return gross_prices

def generate_customers(row_count: int) -> list[dict]:
    customers: list[dict] = []
    batch_id = generate_batch_id()
    load_timestamp = current_load_timestamp()

    for _ in range(row_count):
        store = random.choice(list(ChildCompanyEnum)).value
        city = random.choice(list(CITY_TO_PROVINCE.keys()))
        province = CITY_TO_PROVINCE[city]

        customers.append({
            "customer_code": _customer_code(),
            "customer_name": fake.name(),
            "customer_type": random.choice(list(CustomerTypeEnum)).value,
            "channel": random.choice(list(ChannelEnum)).value,
            "store_name": store.store_name,
            "store_code": store.code,
            "city": city,
            "province": province,
            "source_system": random.choice([SourceSystemEnum.CRM]).value,
            "load_timestamp": load_timestamp,
            "batch_id": batch_id,
        })

    return customers

def generate_orders(row_count: int, customers: list[dict], products: list[dict], gross_prices: list[dict], chunk_size: int = 0) -> list[dict]:
    orders: list[dict] = []
    batch_id = generate_batch_id()
    load_timestamp = current_load_timestamp()

    order_dates = _generate_order_dates(row_count, chunk_size)

    for i in range(row_count):
        customer = random.choice(customers)
        product = random.choice(products)
        gross_price = random.choice(gross_prices)
        order_date = order_dates[i]
        
        # Late Arrival Injection: ~2% of orders have their load_timestamp delayed
        order_load_timestamp = load_timestamp
        if random.random() < 0.02:
            # Delay the load timestamp by 1 to 5 days after the order date
            # Assuming current time is date of the run, this simulates late arriving data
            try:
                if isinstance(order_date, date):
                    dt = datetime.combine(order_date, datetime.min.time())
                else:
                    dt = order_date
                    
                delay_days = random.randint(1, 5)
                late_dt = dt + timedelta(days=delay_days)
                order_load_timestamp = late_dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

        orders.append({
            "order_id": _order_id(),
            "order_date": order_date,
            "order_timestamp": order_date,
            "customer_code": customer["customer_code"],
            "product_sku": product["product_sku"],
            "store_code": customer["store_code"],
            "channel": customer["channel"],
            "quantity": random.randint(10, 999),
            "gross_sales_amount": gross_price["gross_price"],
            "discount_amount": round(gross_price["gross_price"] * 0.03, 2),
            "net_sales_amount": round(gross_price["gross_price"] - (gross_price["gross_price"] * 0.03), 2),
            "payment_method": random.choice(list(PaymentMethodEnum)).value,
            "currency": gross_price["currency"],
            "price_type": gross_price["price_type"],
            "valid_from": gross_price["valid_from"],
            "valid_to": gross_price["valid_to"],
            "source_system":random.choice([SourceSystemEnum.POS,SourceSystemEnum.ONLINE]).value,
            "load_timestamp": order_load_timestamp,
            "batch_id": batch_id,
        })

    return orders

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

def write_orders_by_date(orders: list[dict], path: str, fieldnames: list[str]) -> None:
    orders_by_date = {}
    for order in orders:
        o_date = order["order_date"]
        o_date_str = o_date.strftime("%Y-%m-%d") if isinstance(o_date, (date, datetime)) else o_date
        orders_by_date.setdefault(o_date_str, []).append(order)

    for o_date_str, chunk in orders_by_date.items():
        filename = f"orders_{o_date_str}.csv"
        write_to_csv(chunk, path, filename, fieldnames=fieldnames)
        print(f"Successfully generated {len(chunk)} rows of {filename}")

def main():
    N_PRODUCTS = int(os.getenv("N_PRODUCTS", 5))
    N_CUSTOMERS = int(os.getenv("N_CUSTOMERS", 5))
    N_ORDERS = int(os.getenv("N_ORDERS", 5))
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 4))

    print(f"Generating {N_PRODUCTS} products...")
    products = generate_product(N_PRODUCTS)
    print(f"Generating {N_CUSTOMERS} customers...")
    customers = generate_customers(N_CUSTOMERS)
    print(f"Generating {N_ORDERS} orders...")
    gross_prices = generate_gross_price(products)
    orders = generate_orders(N_ORDERS, customers, products, gross_prices, CHUNK_SIZE)
    
    # Inject Data Issues
    print("Injecting data issues...")
    products = inject_data_issues(products, nullable_fields=["brand", "division", "standard_cost", "pack_size"])
    customers = inject_data_issues(customers, nullable_fields=["city", "province", "channel", "store_name"])
    gross_prices = inject_data_issues(gross_prices, nullable_fields=["currency", "price_type"])
    orders = inject_data_issues(orders, nullable_fields=["discount_amount", "payment_method", "quantity", "channel"])
    
    PRODUCT_FIELDS = ["product_sku", "product_name", "brand", "division", "category", "pack_size", "variant","standard_cost", "supplier_name", "source_system", "load_timestamp", "batch_id"]
    CUSTOMER_FIELDS = ["customer_code", "customer_name", "customer_type", "channel", "store_code","store_name", "city", "province", "source_system", "load_timestamp", "batch_id"]
    GROSS_PRICE_FIELDS = ["product_sku", "store_code", "gross_price", "currency", "price_type", "valid_from", "valid_to", "source_system", "load_timestamp", "batch_id"]
    ORDER_FIELDS = ["order_id", "order_date", "order_timestamp", "customer_code", "product_sku", "store_code", "channel", "quantity", "gross_sales_amount", "discount_amount", "net_sales_amount", "payment_method", "currency", "price_type", "valid_from", "valid_to", "source_system", "load_timestamp", "batch_id"]
   
    out_dir = "../data/raw_data"
    
    files_to_export = [
        (products, "products.csv", PRODUCT_FIELDS),
        (customers, "customers.csv", CUSTOMER_FIELDS),
        (gross_prices, "gross_prices.csv", GROSS_PRICE_FIELDS)
    ]
    
    for data, filename, fields in files_to_export:
        write_to_csv(data, out_dir, filename, fieldnames=fields)
        print(f"Successfully generated {len(data)} rows of {filename}")

    write_orders_by_date(orders, out_dir, ORDER_FIELDS)

if __name__ == "__main__":
    main()
