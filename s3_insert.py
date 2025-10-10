import boto3
import subprocess
import json
import sys
from datetime import datetime

# ---------------------------
# CONFIG - Runs data generators to push into AWS S3
# ---------------------------
BUCKET_NAME = "yuggietl"
ORDERS_KEY_PREFIX = "raw/orders/"
CARBON_KEY_PREFIX = "raw/carbon_emissions/"

# location of your generators
ORDERS_SCRIPT = "Orders_generator.py"
CARBON_SCRIPT = "Carbon_footprint_generator.py"

# S3 client
s3 = boto3.client("s3")

def run_generator(script, count):
    """Run a generator script and capture JSON lines output."""
    result = subprocess.run(
        ["python", script, str(count)],
        capture_output=True,
        text=True,
        check=True
    )
    # each line is JSON
    lines = [line for line in result.stdout.strip().split("\n") if line.strip()]
    return lines

def upload_to_s3(lines, prefix, dataset_name):
    """Upload JSONL to S3 with timestamped key."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}{dataset_name}_{timestamp}.jsonl"

    body = "\n".join(lines)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8")
    )
    print(f"✅ Uploaded {len(lines)} records to s3://{BUCKET_NAME}/{key}")

if __name__ == "__main__":
    # Read command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python s3_insert.py <num_orders> <num_carbon_records>")
        sys.exit(1)

    ORDER_COUNT = int(sys.argv[1])
    CARBON_COUNT = int(sys.argv[2])

    # Generate & upload orders
    order_lines = run_generator(ORDERS_SCRIPT, ORDER_COUNT)
    upload_to_s3(order_lines, ORDERS_KEY_PREFIX, "orders")

    # Generate & upload carbon emissions
    carbon_lines = run_generator(CARBON_SCRIPT, CARBON_COUNT)
    upload_to_s3(carbon_lines, CARBON_KEY_PREFIX, "carbon_emissions")
