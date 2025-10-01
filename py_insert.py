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
    row = (record['txid'],record['rfid'],record["item"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],json.dumps(record['address']),record['phone'],record['email'],json.dumps(record['emergency_contact']))
    # this dataset has variant records, so utilizing an executemany() is not possible, must insert 1 record at a time
    snow.cursor().execute("INSERT INTO CLIENT_SUPPORT_ORDERS (\"TXID\",\"RFID\",\"ITEM\",\"PURCHASE_TIME\", \"EXPIRATION_TIME\",\"DAYS\",\"NAME\",\"ADDRESS\",\"PHONE\",\"EMAIL\",\"EMERGENCY_CONTACT\") SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?)", row)
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
