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
    "Single-Origin Spotlight": 14.99,
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
    "Single-Origin Spotlight", "Decaf Dream Blend", "Cold Brew Blend",
    "Espresso Roast", "Customizable Blend", "Limited Edition Seasonal Blends",
    "Organic Matcha Powder", "Golden Turmeric Latte Mix", "Chai Tea Blend",
    "Herbal Relaxation Blend", "Instant Coffee Sticks"
]

# origin country mapping for coffee/tea items
origin_map = {
    "Signature House Blend": "Colombia",
    "Bold Awakening Blend": "Ethiopia",
    "Golden Morning Blend": "Guatemala",
    "Single-Origin Spotlight": "Kenya",
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

def assign_warehouse(region):
    if region in ["North America", "South America"]:
        return random.choice(["East Coast USA", "West Coast USA"])
    else:
        return "Paris"

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

    # pick region first
    region = random.choice(list(regions.keys()))
    address = generate_address(region)
    warehouse = assign_warehouse(region)

    # pick one item
    item_name = random.choice(list(price_map.keys()))
    base_price = price_map[item_name]
    quantity = random.randint(1, 3)

    # assign bag size
    if item_name in coffee_items:
        size = random.choice(["250g", "500g", "1kg"])
        multiplier = bag_size_multiplier[size]
    else:
        size = "N/A"
        multiplier = 1.0

    unit_price_adjusted = round(base_price * multiplier, 2)
    total_price = round(unit_price_adjusted * quantity, 2)

    # shipping method and delivery status
    shipping_method = random.choice(["Standard", "Express", "EcoDelivery", "Local Pickup"])
    order_age_days = (datetime.utcnow() - purchase_dt).days
    status_options = ["Delivered", "Returned", "Canceled"]
    if order_age_days <= 30:
        status_options.append("In Transit")
    delivery_status = random.choice(status_options)

    client_support = {
        "txid": str(uuid.uuid4()),
        "rfid": hex(random.getrandbits(96)),
        "item": item_name,
        "bag_size": size,
        "unit_price": unit_price_adjusted,
        "quantity": quantity,
        "total_price": total_price,
        "origin_country": origin_map[item_name] if item_name in origin_map else "N/A",
        "purchase_time": purchase_time_iso,
        "region": region,
        "name": fake.name(),
        "address": address,
        "phone": fake.phone_number(),
        "email": fake.email(),
        "warehouse": warehouse,  
        "shipping_method": shipping_method,
        "delivery_status": delivery_status
    }

    d = json.dumps(client_support) + "\n"
    sys.stdout.write(d)



if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print('')