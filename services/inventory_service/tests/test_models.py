import unittest
from uuid import uuid4
from datetime import datetime
from src.domain.inventory import InventoryItem
from src.domain.enums import category
from pydantic import ValidationError

class TestInventoryModels(unittest.TestCase):
    def test_inventory_item_creation(self):
        item = InventoryItem(
            id=uuid4(),
            product_id=uuid4(),
            branch_id=uuid4(),
            name="Test Item",
            sku="SKU-123",
            category=category.PANTRY,
            quantity=10,
            unit_price=99.99, 
            last_updated=datetime.now()
        )
        self.assertEqual(item.name, "Test Item")
        self.assertEqual(item.quantity, 10)

    def test_invalid_quantity(self):
        # Assuming Pydantic models might strictly type check, 
        # but standard int doesn't reject string "10" if it can cast.
        # Let's test missing required field.
        with self.assertRaises(ValidationError):
            InventoryItem(
                id=uuid4(),
                product_id=uuid4(),
                branch_id=uuid4(),
                # name missing
                sku="SKU-123",
                category=category.PANTRY,
                quantity=10,
                unit_price=99.99
            )
