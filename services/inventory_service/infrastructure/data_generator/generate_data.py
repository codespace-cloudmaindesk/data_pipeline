import asyncio
import uuid
import random
from datetime import datetime, timedelta
from typing import List
from faker import Faker

from src.domain.inventory import InventoryItem
from src.domain.movement import InventoryMovement
from src.domain.analytics import InventorySnapshot
from src.domain.enums import movement_type

from src.core.inventory_service import inventory_service
from src.repositories.postgres_repo import init_postgres, insert_inventory_snapshots
from src.repositories.scylla_repo import init_scylla, insert_inventory_snapshots_by_item_batch, update_inventory_snapshot_by_branch
from src.events.producer import start_producer, stop_producer

from src.lib.utils import random_department_category, random_branch

NUM_ITEMS = 50
MOVEMENTS_PER_ITEM = 5
MOVEMENTS_PER_ITEM = 5
NUM_DAYS_SNAPSHOT = 7

fake = Faker()

class InventoryDataGenerator:
    """Generates demo inventory data efficiently with batch inserts and branch-level KPIs."""

    async def generate_items_batch(self) -> List[InventoryItem]:
        items = []

        for _ in range(NUM_ITEMS):
            branch = random_branch()
            dept, cat = random_department_category()
            item_id = uuid.uuid4()

            item = InventoryItem(
                id=item_id,
                product_id=uuid.uuid4(),
                branch_id=uuid.UUID(branch.value),
                name=f"{fake.word()}-{cat.value}",
                sku=f"{fake.lexify('SKU-??????')}",
                category=cat.value,
                department=dept.value,

                quantity=random.randint(10, 100),
                unit_price=round(random.uniform(10.0, 100.0), 2),
                threshold=10,
                last_updated=datetime.now()
            )
            items.append(item)

        # Batch insert
        await inventory_service.add_items_batch(items)
        print(f"Inserted {len(items)} items in batch.")
        return items

    async def generate_movements_batch(self, items: List[InventoryItem]):
        movements = []

        for item in items:
            for _ in range(MOVEMENTS_PER_ITEM):
                movement = InventoryMovement(
                    id=uuid.uuid4(),
                    transaction_id=uuid.uuid4(),
                    item_id=item.id,
                    branch_id=item.branch_id,
                    movement_type=random.choice(list(movement_type)).value,
                    quantity=random.randint(1, 10),
                    timestamp=fake.date_time_this_year(),
                    reason=fake.sentence(nb_words=3)
                )
                movements.append(movement)

        # Batch insert movements
        await inventory_service.add_movements_batch(movements)
        print(f"Inserted {len(movements)} movements in batch.")

        # Update branch-level KPIs (turnover & stock value)
        today = datetime.now().date()
        branch_totals = {}
        for m in movements:
            if m.branch_id not in branch_totals:
                branch_totals[m.branch_id] = {"quantity": 0, "stock_value": 0}
            branch_totals[m.branch_id]["quantity"] += m.quantity
            # stock_value approximate: using same unit_price for demo
            branch_totals[m.branch_id]["stock_value"] += m.quantity * item.unit_price

        # Update branch snapshots in Scylla
        for branch_id, totals in branch_totals.items():
            await update_inventory_snapshot_by_branch(
                snapshot_date=today,
                branch_id=uuid.UUID(branch_id),
                quantity=totals["quantity"],
                stock_value=totals["stock_value"]
            )

    async def generate_snapshots(self, items: List[InventoryItem]):
        today = datetime.now().date()
        snapshots = []

        for item in items:
            for i in range(NUM_DAYS_SNAPSHOT):
                snap_date = today - timedelta(days=i)
                snapshot = InventorySnapshot(
                    snapshot_date=snap_date,
                    item_id=item.id,
                    branch_id=item.branch_id,
                    quantity=random.randint(0, 100),
                    unit_price=item.unit_price,
                    stock_value=random.randint(0, 100) * item.unit_price,
                    total_value=random.randint(0, 100) * item.unit_price,
                    out_of_stock=False
                )
                snapshots.append(snapshot)

        # Batch insert snapshots in Postgres (analytics)
        await insert_inventory_snapshots(snapshots)
        # Optionally, also insert in Scylla read-optimized tables
        await insert_inventory_snapshots_by_item_batch(snapshots)
        print(f"Inserted {len(snapshots)} analytics snapshots.")

    async def generate_all(self):
        print("Initializing infrastructure...")
        init_scylla()
        await init_postgres()
        await start_producer()

        items = await self.generate_items_batch()
        await self.generate_movements_batch(items)
        await self.generate_snapshots(items)

        await stop_producer()
        print("Data generation complete.")

if __name__ == "__main__":
    generator = InventoryDataGenerator()
    asyncio.run(generator.generate_all())
