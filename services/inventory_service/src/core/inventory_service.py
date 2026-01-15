from datetime import datetime
from typing import List

from src.domain.inventory import InventoryItem
from src.domain.movement import InventoryMovement
from src.repositories import scylla_repo
from src.events.producer import publish_event


class InventoryService:
    """Handles inventory writes and event publishing."""

    # ---------- Single item ----------

    async def add_item(self, item: InventoryItem):
        await scylla_repo.insert_inventory_item(item)

        await publish_event({
            "event_type": "ITEM_CREATED",
            "data": item.dict(exclude={"last_updated"}),
            "timestamp": datetime.utcnow().isoformat()
        })

    async def add_movement(self, movement: InventoryMovement):
        await scylla_repo.insert_inventory_movement(movement)

        await publish_event({
            "event_type": "INVENTORY_MOVEMENT",
            "data": movement.dict(exclude={"timestamp"}),
            "timestamp": movement.timestamp.isoformat()
        })

    # ---------- Batch ----------

    async def add_items_batch(self, items: List[InventoryItem]):
        if not items:
            return

        await scylla_repo.insert_inventory_items_batch(items)

        await publish_event({
            "event_type": "ITEMS_CREATED_BATCH",
            "count": len(items),
            "timestamp": datetime.utcnow().isoformat()
        })

    async def add_movements_batch(self, movements: List[InventoryMovement]):
        if not movements:
            return

        await scylla_repo.insert_inventory_movements_batch(movements)

        await publish_event({
            "event_type": "INVENTORY_MOVEMENTS_BATCH",
            "count": len(movements),
            "timestamp": datetime.utcnow().isoformat()
        })


inventory_service = InventoryService()
