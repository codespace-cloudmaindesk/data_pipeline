import psycopg2
import os

class PostgresRepo:
    def __init__(self, batch_size=100):
        self.host = os.getenv("POSTGRES_HOST", "postgres")
        self.dbname = os.getenv("POSTGRES_DB", "inventory_db")
        self.user = os.getenv("POSTGRES_USER", "inventory")
        self.password = os.getenv("POSTGRES_PASSWORD", "inventory123")
        self.batch_size = batch_size
        self.batch_count = 0
        self.conn = None
        self.cur = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cur = self.conn.cursor()
        self._initialize_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        self.conn.close()

    def _initialize_tables(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_analytics (
                store_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                quantity INT NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (store_id, sku)
            )
        """)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_adjustments (
                id SERIAL PRIMARY KEY,
                store_id TEXT NOT NULL,
                sku TEXT NOT NULL,
                quantity INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def upsert_stock(self, store_id, sku, quantity):
        """Insert or update record, commit in batches."""
        self.cur.execute("""
            INSERT INTO stock_analytics (store_id, sku, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (store_id, sku)
            DO UPDATE SET
                quantity = EXCLUDED.quantity,
                last_updated = CURRENT_TIMESTAMP
        """, (store_id, sku, quantity))
        self.batch_count += 1
        if self.batch_count >= self.batch_size:
            self.commit_batch()

    def commit_batch(self):
        if self.batch_count > 0:
            self.conn.commit()
            self.batch_count = 0

    def log_adjustment(self, store_id, sku, quantity):
        self.cur.execute(
            "INSERT INTO stock_adjustments (store_id, sku, quantity) VALUES (%s, %s, %s)",
            (store_id, sku, quantity)
        )
        self.conn.commit()
