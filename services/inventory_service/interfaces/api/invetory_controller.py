from infrastructure.persistence.scylla_repo import ScyllaRepo
from infrastructure.persistence.postgres_repo import PostgresRepo

class InventoryController:
    def __init__(self):
        self.scylla = ScyllaRepo()
        self.postgres = PostgresRepo()

    def get_stock(self):
        return self.scylla.get_stock()

    def adjust_stock(self, sku, quantity):
        success = self.scylla.adjust_stock(sku, quantity)
        self.postgres.log_adjustment(sku, quantity)
        return {"success": success, "sku": sku, "quantity": quantity}
