import os
import asyncpg
from typing import List
from src.domain.analytics import InventorySnapshot

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "inventory_analytics")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

async def get_postgres_conn():
    return await asyncpg.connect(DSN)

async def init_postgres():
    # We might need to connect to default DB to create the target DB if it doesn't exist
    # For now assuming DB exists or is created by docker-compose
    try:
        conn = await get_postgres_conn()
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory_daily_snapshot (
                snapshot_date DATE,
                item_id UUID,
                branch_id UUID,
                quantity INT,
                unit_price NUMERIC,
                stock_value NUMERIC,
                PRIMARY KEY (snapshot_date, item_id, branch_id)
            );
        """)
        
        await conn.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS stock_value_trend AS
            SELECT snapshot_date, SUM(stock_value) total_value
            FROM inventory_daily_snapshot
            GROUP BY snapshot_date;
        """)
        
        await conn.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS branch_stock_comparison AS
            SELECT branch_id, SUM(quantity) total_stock
            FROM inventory_daily_snapshot
            GROUP BY branch_id;
        """)
        
        await conn.close()
    except Exception as e:
        print(f"Error initializing Postgres: {e}")

async def insert_inventory_snapshot(snapshot: InventorySnapshot):
    conn = await get_postgres_conn()
    try:
        await conn.execute("""
            INSERT INTO inventory_daily_snapshot 
            (snapshot_date, item_id, branch_id, quantity, unit_price, stock_value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (snapshot_date, item_id, branch_id) DO UPDATE 
            SET quantity = EXCLUDED.quantity, 
                stock_value = EXCLUDED.stock_value;
        """, snapshot.snapshot_date, snapshot.item_id, snapshot.branch_id, 
           snapshot.quantity, snapshot.unit_price, snapshot.stock_value)
    finally:
        await conn.close()

async def insert_inventory_snapshots(snapshots: List[InventorySnapshot]):
    conn = await get_postgres_conn()
    try:
        # asyncpg executemany using unnest approach for UPSERT is tricky.
        # But we can just loop for now or use copy_records_to_table if no conflict handling needed.
        # But we need ON CONFLICT DO UPDATE.
        # So we can use a loop or batch execute.
        # Efficient batch upsert in Postgres usually involves unnest.
        # For simplicity in 'debug' mode, loop is acceptable or executemany.
        
        records = [
            (s.snapshot_date, s.item_id, s.branch_id, s.quantity, s.unit_price, s.stock_value)
            for s in snapshots
        ]
        
        await conn.executemany("""
            INSERT INTO inventory_daily_snapshot 
            (snapshot_date, item_id, branch_id, quantity, unit_price, stock_value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (snapshot_date, item_id, branch_id) DO UPDATE 
            SET quantity = EXCLUDED.quantity, 
                stock_value = EXCLUDED.stock_value;
        """, records)
    finally:
        await conn.close()
