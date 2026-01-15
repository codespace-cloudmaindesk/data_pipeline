from fastapi import APIRouter, HTTPException
from typing import List

from src.domain.inventory import InventoryItem
from src.domain.movement import InventoryMovement
from src.core.inventory_service import inventory_service

router = APIRouter(prefix="/inventory", tags=["Inventory"])

# Add a new inventory item
@router.post("/items", response_model=dict)
async def create_inventory_item(item: InventoryItem):
    try:
        await inventory_service.add_item(item)
        return {"status": "success", "item_id": str(item.id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add multiple inventory items
@router.post("/items/batch", response_model=dict)
async def create_inventory_items_batch(items: List[InventoryItem]):
    try:
        await inventory_service.add_items_batch(items)
        return {"status": "success", "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add a new inventory movement
@router.post("/movements", response_model=dict)
async def create_inventory_movement(movement: InventoryMovement):
    try:
        await inventory_service.add_movement(movement)
        return {"status": "success", "movement_id": str(movement.id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Add multiple inventory movements
@router.post("/movements/batch", response_model=dict)
async def create_inventory_movements_batch(movements: List[InventoryMovement]):
    try:
        await inventory_service.add_movements_batch(movements)
        return {"status": "success", "count": len(movements)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
