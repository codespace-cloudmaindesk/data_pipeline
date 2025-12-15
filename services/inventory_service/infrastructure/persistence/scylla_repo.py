from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory
from datetime import datetime
import os

class ScyllaRepo:
    def __init__(self, host="scylla", batch_size=100):
        self.host = os.getenv("SCYLLA_HOST", host)
        self.batch_size = batch_size
        self.cluster = None
        self.session = None
        self.batch = None
        self.prepared_statement = None
        self._batch_count = 0

    def __enter__(self):
        profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.ONE,
            load_balancing_policy=RoundRobinPolicy(),
            request_timeout=10,
            row_factory=dict_factory
        )

        self.cluster = Cluster(
            [self.host],
            protocol_version=4,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile}
        )

        self.session = self.cluster.connect()
        self._initialize_keyspace_and_tables()

        self.prepared_statement = self.session.prepare("""
            INSERT INTO inventory.stock (store_id, sku, quantity, last_updated)
            VALUES (?, ?, ?, ?)
        """)

        self.batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.execute_batch()
        self.session.shutdown()
        self.cluster.shutdown()

    def _initialize_keyspace_and_tables(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS inventory
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
        """)
        self.session.set_keyspace("inventory")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stock (
                store_id text,
                sku text,
                quantity int,
                last_updated timestamp,
                PRIMARY KEY (store_id, sku)
            )
        """)

    def save_stock(self, store_id, sku, quantity):
        self.batch.add(
            self.prepared_statement,
            (store_id, sku, quantity, datetime.utcnow())
        )
        self._batch_count += 1

        if self._batch_count >= self.batch_size:
            self.execute_batch()

    def execute_batch(self):
        if self.batch and len(self.batch) > 0:
            self.session.execute(self.batch)
            self.batch.clear()
        self._batch_count = 0
