from flask import Blueprint, render_template
from interfaces.api.inventory_controller import InventoryController

ui = Blueprint('ui', __name__)
controller = InventoryController()

@ui.route("/dashboard")
def dashboard():
    stock = controller.get_stock()
    return render_template("dashboard.html", stock=stock)
