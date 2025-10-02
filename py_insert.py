import os, sys, logging
import json
import snowflake.connector

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'}, 
    )


def save_to_snowflake(snow, message):
    record = json.loads(message)
    logging.debug('inserting record to db')

    row = (
        record['txid'],
        record['rfid'],
        record['customer_id'],
        record['product_id'],
        record['item'],
        record.get('bag_size'),
        record.get('unit_price'),
        record.get('quantity'),
        record.get('total_price'),
        record.get('origin_country'),
        record['purchase_time'],
        record.get('shipped_date'),
        record.get('delivered_date'),
        record.get('region'),
        record.get('name'),
        record.get('street_address'),
        record.get('city'),
        record.get('country'),
        record.get('postalcode'),
        record.get('phone'),
        record.get('email'),
        record.get('warehouse'),
        record.get('shipping_method'),
        record.get('delivery_status'),
        record.get('payment_method'),
        record.get('payment_status'),
        record.get('fair_trade_certified'),
        record.get('organic_certified'),
        record.get('carbon_score'),
        record.get('delivery_delay_days'),
    )

    sql = """
        INSERT INTO CLIENT_SUPPORT_ORDERS (
            "TXID","RFID","CUSTOMER_ID","PRODUCT_ID","ITEM","BAG_SIZE","UNIT_PRICE","QUANTITY",
            "TOTAL_PRICE","ORIGIN_COUNTRY","PURCHASE_TIME","SHIPPED_DATE","DELIVERED_DATE","REGION",
            "NAME","STREET_ADDRESS","CITY","COUNTRY","POSTALCODE","PHONE","EMAIL","WAREHOUSE",
            "SHIPPING_METHOD","DELIVERY_STATUS","PAYMENT_METHOD","PAYMENT_STATUS",
            "FAIR_TRADE_CERTIFIED","ORGANIC_CERTIFIED","CARBON_SCORE","DELIVERY_DELAY_DAYS"
        )
        SELECT ?,?,?,?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?,?
    """
    snow.cursor().execute(sql, row)
    logging.debug(f"inserted order {record}")


if __name__ == "__main__":    
    snow = connect_snow()
    for message in sys.stdin:
        if message != '\n':
            save_to_snowflake(snow, message)
        else:
            break
    snow.close()
    logging.info("Ingest complete")
