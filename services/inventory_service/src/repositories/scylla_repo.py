import os
from uuid import UUID
from datetime import date
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from typing import List

from src.domain.inventory import InventoryItem
from src.domain.movement import InventoryMovement
from src.domain.analytics import InventorySnapshot

# ----------------------------
# Config
# ----------------------------
SCYLLA_HOSTS = os.getenv("SCYLLA_HOSTS", "localhost").split(",")
SCYLLA_USERNAME = os.getenv("SCYLLA_USERNAME", "cassandra")
SCYLLA_PASSWORD = os.getenv("SCYLLA_PASSWORD", "cassandra")
KEYSPACE = "inventory_ks"

_session = None

# ----------------------------
# Session
# ----------------------------
def get_session():
    global _session
    if _session:
        return _session
    auth = PlainTextAuthProvider(username=SCYLLA_USERNAME, password=SCYLLA_PASSWORD)
    cluster = Cluster(SCYLLA_HOSTS, auth_provider=auth)
    _session = cluster.connect()
    return _session

# ----------------------------
# Init
# ----------------------------
def init_scylla():
    session = get_session()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    session.set_keyspace(KEYSPACE)

    # Inventory tables
    session.execute("""
        CREATE TABLE IF NOT EXISTS inventory_by_item (
            item_id UUID,
            branch_id UUID,
            name TEXT,
            quantity INT,
            unit_price DECIMAL,
            last_updated TIMESTAMP,
            PRIMARY KEY ((item_id), branch_id)
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inventory_movements_by_item (
            item_id UUID,
            movement_time TIMESTAMP,
            movement_type TEXT,
            quantity INT,
            branch_id UUID,
            transaction_id UUID,
            reason TEXT,
            PRIMARY KEY ((item_id), movement_time)
        ) WITH CLUSTERING ORDER BY (movement_time DESC);
    """)

    # Analytics tables
    session.execute("""
        CREATE TABLE IF NOT EXISTS inventory_snapshot_by_item_day (
            snapshot_date DATE,
            item_id UUID,
            branch_id UUID,
            quantity INT,
            stock_value DECIMAL,
            PRIMARY KEY ((snapshot_date), item_id, branch_id)
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inventory_snapshot_by_branch_day (
            snapshot_date DATE,
            branch_id UUID,
            total_quantity INT,
            total_stock_value DECIMAL,
            PRIMARY KEY ((snapshot_date), branch_id)
        );
    """)

# ----------------------------
# Inventory CRUD
# ----------------------------
async def insert_inventory_item(item: InventoryItem):
    get_session().execute(
        SimpleStatement("""
            INSERT INTO inventory_by_item
            (item_id, branch_id, name, quantity, unit_price, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
        """),
        (item.id, item.branch_id, item.name, item.quantity, item.unit_price, item.last_updated)
    )

async def insert_inventory_items_batch(items: List[InventoryItem]):
    for item in items:
        await insert_inventory_item(item)

async def insert_inventory_movement(movement: InventoryMovement):
    get_session().execute(
        SimpleStatement("""
            INSERT INTO inventory_movements_by_item
            (item_id, movement_time, movement_type, quantity, branch_id, transaction_id, reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """),
        (
            movement.item_id,
            movement.timestamp,
            movement.movement_type.value,
            movement.quantity,
            movement.branch_id,
            movement.transaction_id,
            movement.reason
        )
    )

async def insert_inventory_movements_batch(movements: List[InventoryMovement]):
    for m in movements:
        await insert_inventory_movement(m)

# ----------------------------
# Analytics
# ----------------------------
async def insert_inventory_snapshots_by_item_batch(snapshots: List[InventorySnapshot]):
    session = get_session()
    for s in snapshots:
        session.execute(
            SimpleStatement("""
                INSERT INTO inventory_snapshot_by_item_day
                (snapshot_date, item_id, branch_id, quantity, stock_value)
                VALUES (%s, %s, %s, %s, %s)
            """),
            (s.snapshot_date, s.item_id, s.branch_id, s.quantity, s.stock_value)
        )

async def update_inventory_snapshot_by_branch(snapshot_date: date, branch_id: UUID, quantity: int, stock_value: float):
    session = get_session()
    row = session.execute(
        SimpleStatement("""
            SELECT total_quantity, total_stock_value
            FROM inventory_snapshot_by_branch_day
            WHERE snapshot_date=%s AND branch_id=%s
        """),
        (snapshot_date, branch_id)
    ).one()

    total_quantity = quantity
    total_stock_value = stock_value
    if row:
        total_quantity += row.total_quantity
        total_stock_value += float(row.total_stock_value)

    session.execute(
        SimpleStatement("""
            INSERT INTO inventory_snapshot_by_branch_day
            (snapshot_date, branch_id, total_quantity, total_stock_value)
            VALUES (%s, %s, %s, %s)
        """),
        (snapshot_date, branch_id, total_quantity, total_stock_value)
    )
