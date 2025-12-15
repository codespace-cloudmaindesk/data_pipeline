from flask import Flask, jsonify, request
from interfaces.api.inventory_controller import InventoryController

app = Flask(__name__)
controller = InventoryController()

@app.route("/")
def index():
    return jsonify({"message": "Inventory Management System is running"})

@app.route("/products")
def get_products():
    return jsonify(controller.get_stock())

@app.route("/adjust_stock", methods=["POST"])
def adjust_stock():
    data = request.get_json()
    result = controller.adjust_stock(data["sku"], data["quantity"])
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)