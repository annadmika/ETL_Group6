import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv

load_dotenv()
import random, uuid, json, sys
from datetime import datetime, timedelta, date
from faker import Faker

fake = Faker()

# full inventory price map
price_map = {
    "Signature House Blend": 12.99,
    "Bold Awakening Blend": 13.99,
    "Golden Morning Blend": 11.49,
    "Single-Origin Spotlight Blend": 14.99,
    "Decaf Dream Blend": 12.49,
    "Cold Brew Blend": 15.99,
    "Espresso Roast": 13.49,
    "Customizable Blend": 16.99,
    "Limited Edition Seasonal Blends": 18.99,
    "Coffee Sampler Box": 22.99,
    "Organic Matcha Powder": 19.99,
    "Golden Turmeric Latte Mix": 17.99,
    "Chai Tea Blend": 12.99,
    "Herbal Relaxation Blend": 11.99,
    "Nitro Cold Brew Cans": 3.49,
    "Oat Latte Cans": 3.99,
    "Instant Coffee Sticks": 7.99,
    "Reusable Coffee Canisters": 24.99,
    "Compostable Coffee Filters": 9.99,
    "French Press (Eco-Glass + Bamboo Lid)": 34.99,
    "Pour-Over Set": 29.99,
    "Travel Mugs / Tumblers": 21.99,
    "Reusable Cold Brew Bottle": 25.99,
    "Coffee Scoops (Bamboo or Stainless Steel with Clip)": 8.99,
    "Branded Tote Bags": 14.99,
    "Coffee Grounds Body Scrub": 18.49
}

# coffee/tea products that use bag size multipliers
coffee_items = [
    "Signature House Blend", "Bold Awakening Blend", "Golden Morning Blend",
    "Single-Origin Spotlight Blend", "Decaf Dream Blend", "Cold Brew Blend",
    "Espresso Roast", "Customizable Blend", "Limited Edition Seasonal Blends",
    "Organic Matcha Powder", "Golden Turmeric Latte Mix", "Chai Tea Blend",
    "Herbal Relaxation Blend", "Instant Coffee Sticks"
]

# origin country mapping for coffee/tea items
origin_map = {
    "Signature House Blend": "Colombia",
    "Bold Awakening Blend": "Ethiopia",
    "Golden Morning Blend": "Guatemala",
    "Decaf Dream Blend": "Peru",
    "Cold Brew Blend": "Brazil",
    "Espresso Roast": "Italy",
    "Customizable Blend": "Blend (Multiple Origins)",
    "Limited Edition Seasonal Blends": "Varies by Season",
    "Organic Matcha Powder": "Japan",
    "Golden Turmeric Latte Mix": "India",
    "Chai Tea Blend": "India",
    "Herbal Relaxation Blend": "Various",
    "Instant Coffee Sticks": "Brazil"
}


# bag size multipliers
bag_size_multiplier = {
    "250g": 1.0,
    "500g": 1.8,
    "1kg": 3.5,
    "N/A": 1.0
}

# regions with example countries
regions = {
    "North America": ["United States", "Canada", "Mexico"],
    "South America": ["Brazil", "Argentina", "Colombia", "Chile"],
    "Europe": ["France", "Germany", "Italy", "Spain", "United Kingdom"],
    "Asia": ["Japan", "China", "India", "Vietnam", "South Korea"],
    "Africa": ["Ethiopia", "Kenya", "South Africa", "Nigeria", "Ghana"],
    "Australia": ["Australia", "New Zealand"]
}

def find_region_for_country(country):
    for region_name, countries in regions.items():
        if country in countries:
            return region_name
    return None

# fixed warehouse set: West US, East US, Paris FR, Asia hub
warehouses = {
    "WEST_US": {"name": "West Coast USA", "country": "United States"},
    "EAST_US": {"name": "East Coast USA", "country": "United States"},
    "PARIS_FR": {"name": "Paris", "country": "France"},
    "ASIA_HUB": {"name": "Singapore", "country": "Singapore"},
}

def assign_warehouse(region):
    # Map region to the appropriate warehouse group, then choose where needed
    if region in ["North America", "South America"]:
        choice = random.choice(["WEST_US", "EAST_US"])
    elif region in ["Europe", "Africa"]:
        choice = "PARIS_FR"
    else:  # Asia, Australia
        choice = "ASIA_HUB"
    wh = warehouses[choice]
    return wh["name"], wh["country"]

def generate_address(region):
    country = random.choice(regions[region])
    return {
        "street_address": fake.street_address(),
        "city": fake.city(),
        "country": country,
        "postalcode": fake.postcode()
    }

def print_client_support():
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime.utcnow()
    random_seconds = random.randint(0, int((end_dt - start_dt).total_seconds()))
    purchase_dt = start_dt + timedelta(seconds=random_seconds)
    purchase_time_iso = purchase_dt.isoformat()

    # pick region first, then country from that region, then warehouse based on region
    region = random.choice(list(regions.keys()))
    address = generate_address(region)
    warehouse_name, warehouse_country = assign_warehouse(region)

    # pick one item
    item_name = random.choice(list(price_map.keys()))
    base_price = price_map[item_name]
    quantity = random.randint(1, 3)

    # origin country: special rule for Single-Origin Spotlight -> random coffee origin
    if item_name == "Single-Origin Spotlight Blend":
        origin_country = random.choice([
            "Ethiopia", "Kenya", "Colombia", "Guatemala", "Peru",
            "Brazil", "Costa Rica", "Honduras", "Rwanda"
        ])
    elif item_name in origin_map:
        origin_country = origin_map[item_name]
    else:
        origin_country = "N/A"

    # assign bag size
    if item_name in coffee_items:
        size = random.choice(["250g", "500g", "1kg"])
        multiplier = bag_size_multiplier[size]
    else:
        size = "N/A"
        multiplier = 1.0

    unit_price_adjusted = round(base_price * multiplier, 2)
    total_price = round(unit_price_adjusted * quantity, 2)

    # shipping method constrained by proximity: Local Pickup only if same country as warehouse
    allowed_methods = ["Standard", "Express", "EcoDelivery", "Local Pickup"]
    if address["country"] != warehouse_country:
        allowed_methods = [m for m in allowed_methods if m != "Local Pickup"]
    shipping_method = random.choice(allowed_methods)

    order_age_days = (datetime.utcnow() - purchase_dt).days
    status_options = ["Delivered", "Returned", "Canceled"]
    if order_age_days <= 30:
        status_options.append("In Transit")
    delivery_status = random.choice(status_options)

    # customer_id: deterministic by email for dedup
    # generate email first to make ID stable across duplicates
    name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    customer_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, email))

    # product_id: deterministic by product name
    product_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, item_name))

    # sustainability: consistent by item
    is_coffee_or_tea = item_name in coffee_items
    fair_trade_certified = is_coffee_or_tea and random.random() < 0.8
    organic_certified = is_coffee_or_tea and ("Organic" in item_name or random.random() < 0.5)

    # carbon score (kg CO2) — depends on distance, method, size, quantity, and status
    # distance factor from warehouse_country to address country
    dest_country = address["country"]
    wh_region = find_region_for_country(warehouse_country) or "Unknown"
    dest_region = find_region_for_country(dest_country) or "Unknown"

    if dest_country == warehouse_country:
        distance_factor = 0.2            # same-country, very low
    elif wh_region == dest_region:
        distance_factor = 1.0            # same-region/continent
    else:
        distance_factor = 1.8            # inter-regional

    # shipping method factor
    if shipping_method == "Local Pickup":
        method_factor = 0.2
    elif shipping_method == "EcoDelivery":
        method_factor = 0.8
    elif shipping_method == "Express":
        method_factor = 1.4
    else:  # Standard
        method_factor = 1.0

    # size factor
    size_factor = 0.0
    if size == "500g":
        size_factor = 0.15
    elif size == "1kg":
        size_factor = 0.35

    # status multiplier
    # - Canceled: lower (not shipped or minimal handling)
    # - Returned: higher (two legs)
    # - Delivered/In Transit: normal
    if delivery_status == "Canceled":
        status_multiplier = 0.3
    elif delivery_status == "Returned":
        status_multiplier = 2.0
    else:
        status_multiplier = 1.0

    # base unit, then scale by factors and quantity
    base_unit = 1.0
    carbon_score = base_unit * distance_factor * method_factor * (1 + size_factor) * quantity * status_multiplier
    carbon_score = round(carbon_score, 2)

    # timestamps: shipped/delivered with constraints
    shipped_dt = None
    delivered_dt = None

    if delivery_status == "Canceled":
        shipped_dt = None
        delivered_dt = None
    elif delivery_status == "In Transit":
        # shipped within 0-5 days after purchase
        shipped_dt = purchase_dt + timedelta(days=random.randint(0, 5))
        delivered_dt = None
    elif delivery_status in ["Delivered", "Returned"]:
        # shipped within 0-3 days, delivered within 1-20 days after shipped (cap 30)
        shipped_dt = purchase_dt + timedelta(days=random.randint(0, 3))
        max_deliver_days = min(30, max(1, order_age_days - (shipped_dt - purchase_dt).days))
        deliver_days = random.randint(1, max(1, min(20, max_deliver_days)))
        delivered_dt = shipped_dt + timedelta(days=deliver_days)
    # hard cap: delivered not more than 1 month after shipped
    if shipped_dt and delivered_dt and delivered_dt > shipped_dt + timedelta(days=30):
        delivered_dt = shipped_dt + timedelta(days=30)

    # payment information
    payment_method = random.choice(["Credit Card", "PayPal", "Gift Card", "Apple Pay", "Google Pay"])
    if delivery_status in ["Canceled", "Returned"]:
        payment_status = "Refunded"
    elif delivery_status == "Delivered":
        payment_status = "Paid"
    elif delivery_status == "In Transit":
        payment_status = "Pending"
    else:
        payment_status = "Paid"

    # delivery metrics
    delivery_delay_days = None
    if delivered_dt:
        delivery_delay_days = (delivered_dt - purchase_dt).days

    client_support = {
        "txid": str(uuid.uuid4()),
        "rfid": hex(random.getrandbits(96)),
        "customer_id": customer_id,
        "product_id": product_id,
        "item": item_name,
        "bag_size": size,
        "unit_price": unit_price_adjusted,
        "quantity": quantity,
        "total_price": total_price,
        "origin_country": origin_country,
        "fair_trade_certified": fair_trade_certified,
        "organic_certified": organic_certified,
        "purchase_time": purchase_dt.date().isoformat(),
        "shipped_date": shipped_dt.date().isoformat() if shipped_dt else None,
        "delivered_date": delivered_dt.date().isoformat() if delivered_dt else None,
        "region": region,
        "name": name,
        "street_address": address["street_address"],
        "city": address["city"],
        "country": address["country"],
        "postalcode": address["postalcode"],
        "phone": phone,
        "email": email,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "warehouse": warehouse_name,
        "shipping_method": shipping_method,
        "delivery_status": delivery_status,
        "delivery_delay_days": delivery_delay_days,
        "carbon_score": carbon_score
    }

    d = json.dumps(client_support) + "\n"
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print('')