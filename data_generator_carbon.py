import sys
import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker


fake = Faker()

# --- Warehouse metadata ---
warehouses = {
    "WEST_US": {"name": "West Coast USA", "country": "United States"},
    "EAST_US": {"name": "East Coast USA", "country": "United States"},
    "PARIS_FR": {"name": "Paris", "country": "France"},
    "ASIA_HUB": {"name": "Asia Hub", "country": "Singapore"},
}

# Coffee origins
coffee_origins = [
    "Colombia", "Ethiopia", "Guatemala", "Peru", "Brazil", "Italy",
    "Japan", "India", "Vietnam", "Kenya", "Costa Rica", "Honduras", "Rwanda"
]

# Distance proxy factors (very rough — simplified)
distance_factors = {
    "Local": 0.2,
    "Regional": 1.0,
    "Intercontinental": 1.8
}

# Shipping method factors
shipping_method_factors = {
    "Local Pickup": 0.2,
    "EcoDelivery": 0.8,
    "Standard": 1.0,
    "Express": 1.4
}

def classify_distance(origin, warehouse_country):
    if origin == warehouse_country:
        return "Local"
    # rough region heuristic
    if (origin in ["Colombia","Brazil","Peru","Costa Rica","Honduras"] and warehouse_country == "United States"):
        return "Regional"
    if (origin in ["France","Italy","Ethiopia","Kenya","Rwanda"] and warehouse_country == "France"):
        return "Regional"
    return "Intercontinental"

def generate_carbon_report():
    reporting_month = fake.date_between(start_date="-1y", end_date="today").replace(day=1)

    # choose origin + warehouse
    origin = random.choice(coffee_origins)
    wh_key = random.choice(list(warehouses.keys()))
    wh = warehouses[wh_key]

    # classify transport
    distance_class = classify_distance(origin, wh["country"])
    distance_factor = distance_factors[distance_class]

    # choose method distribution
    shipping_method = random.choice(list(shipping_method_factors.keys()))
    method_factor = shipping_method_factors[shipping_method]

    # shipments volume
    shipments_count = random.randint(50, 500)  # number of batches in the month
    avg_batch_size = random.uniform(50, 250)   # kg coffee per batch

    # emissions (simplified scaling of your carbon_score logic)
    base_unit = 1.0
    estimated_emissions = round(
        base_unit * distance_factor * method_factor * shipments_count * (avg_batch_size / 100.0),
        2
    )

    return {
        "record_id": str(uuid.uuid4()),
        "reporting_month": reporting_month.isoformat(),
        "warehouse_id": wh_key,
        "warehouse_name": wh["name"],
        "warehouse_country": wh["country"],
        "origin_country": origin,
        "distance_class": distance_class,
        "shipping_method": shipping_method,
        "shipments_count": shipments_count,
        "avg_batch_size_kg": round(avg_batch_size, 2),
        "estimated_emissions_kgCO2e": estimated_emissions
    }

if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0]) if args else 10
    for _ in range(total_count):
        print(json.dumps(generate_carbon_report()))
