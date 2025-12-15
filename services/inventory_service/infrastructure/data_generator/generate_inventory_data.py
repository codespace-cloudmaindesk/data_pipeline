import os
import csv
import json
import random
import time
import logging
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from infrastructure.persistence.scylla_repo import ScyllaRepo
from infrastructure.persistence.postgres_repo import PostgresRepo

# ----------------------------
# Environment & logging setup
# ----------------------------
environment = 'dev' if os.getenv('USERNAME', '') != '' else 'prod'
fake = Faker()
if environment == 'dev':
    Faker.seed(42)
    random.seed(42)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SLEEP_TIME = 0.1 if environment == "dev" else 1
LINE_COUNT = 50       # records per snapshot
FILE_COUNT = 5        # total snapshots
INTERVAL_SEC = 60     # seconds between snapshots
KAFKA_TOPIC = "inventory_topic"
KAFKA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda-0:9092")

# ----------------------------
# Stores, categories, stock
# ----------------------------
STORES = ["Store-001", "Store-002", "Store-003"]
CATEGORIES = {
    "Fruits & Vegetables": ["Apples", "Bananas", "Carrots", "Tomatoes", "Spinach"],
    "Dairy": ["Milk", "Cheese", "Yogurt", "Butter", "Cream"],
    "Bakery": ["Bread", "Buns", "Croissant", "Muffin", "Cake"],
    "Beverages": ["Coke", "Orange Juice", "Water", "Coffee", "Tea"],
    "Snacks": ["Chips", "Chocolate", "Nuts", "Biscuits", "Candy"]
}
STOCK_RANGES = {
    "Fruits & Vegetables": (20, 100),
    "Dairy": (30, 120),
    "Bakery": (10, 50),
    "Beverages": (50, 200),
    "Snacks": (40, 150)
}

# ----------------------------
# Functions
# ----------------------------
def generate_inventory_records(stores, categories, stock_ranges, line_count=LINE_COUNT, snapshot_time=None):
    """Generate a list of inventory records."""
    records = []
    snapshot_time = snapshot_time or datetime.now()
    for _ in range(line_count):
        store = random.choice(stores)
        category = random.choice(list(categories.keys()))
        product = random.choice(categories[category])
        sku = f"{category[:3].upper()}-{product[:3].upper()}-{random.randint(100,999)}"
        min_stock, max_stock = stock_ranges[category]
        quantity = random.randint(min_stock, max_stock)
        records.append({
            "store_id": store,
            "sku": sku,
            "category": category,
            "product": product,
            "quantity": quantity,
            "generated_at": snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
        })
    return records

def save_records_to_csv(records, snapshot_time):
    """Save records to CSV with timestamped filename."""
    filename = f"inventory_data_{snapshot_time.strftime('%Y%m%d_%H%M%S')}.csv"
    fieldnames = ["store_id", "sku", "category", "product", "quantity", "generated_at"]
    with open(filename, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    logger.info(f"Saved {len(records)} records to {filename}")
    return filename

def persist_records(records, sleep_time=SLEEP_TIME):
    """Persist records to ScyllaDB and Postgres."""
    try:
        with ScyllaRepo() as scylla_repo, PostgresRepo() as pg_repo:
            for record in records:
                scylla_repo.save_stock(record["store_id"], record["sku"], record["quantity"])
                pg_repo.upsert_stock(record["store_id"], record["sku"], record["quantity"])
                if sleep_time:
                    time.sleep(sleep_time)
            scylla_repo.execute_batch()
            pg_repo.commit_batch()
        logger.info("Persisted inventory data to Scylla and Postgres")
    except Exception as e:
        logger.error(f"Error persisting data: {e}")

def publish_records_to_kafka(records, topic=KAFKA_TOPIC, broker=KAFKA_BROKER):
    """Publish inventory records to Kafka/Redpanda."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for record in records:
            producer.send(topic, value=record)
        producer.flush()
        logger.info(f"Published {len(records)} records to Kafka topic '{topic}'")
    except Exception as e:
        logger.error(f"Error publishing to Kafka: {e}")

def run_time_series(file_count=FILE_COUNT, interval_sec=INTERVAL_SEC):
    """Generate CSV snapshots, persist to DB, and publish to Kafka."""
    snapshot_time = datetime.now()
    for idx in range(file_count):
        logger.info(f"Generating snapshot {idx + 1}/{file_count} for {snapshot_time}")
        records = generate_inventory_records(STORES, CATEGORIES, STOCK_RANGES, snapshot_time=snapshot_time)
        save_records_to_csv(records, snapshot_time)
        persist_records(records)
        publish_records_to_kafka(records)
        snapshot_time += timedelta(seconds=interval_sec)

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    logger.info("Starting inventory time-series data generation with Kafka...")
    run_time_series()
    logger.info("All snapshots generated and published successfully!")
