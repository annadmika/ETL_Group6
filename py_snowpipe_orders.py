import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)


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
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')

    # Build DataFrame from dict rows (already in target column names)
    pandas_df = pd.DataFrame(batch)

    # Ensure datetime columns are proper timestamps for Parquet → Snowflake
    for col in ["PURCHASE_TIME", "SHIPPED_DATE", "DELIVERED_DATE"]:
        if col in pandas_df.columns:
            pandas_df[col] = pd.to_datetime(pandas_df[col], errors='coerce')

    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"

    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')

    # Put file into the table stage of CLIENT_SUPPORT_ORDERS
    snow.cursor().execute("PUT 'file://{0}' @%CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE".format(out_path))
    os.unlink(out_path)

    # Tell Snowpipe to ingest the new staged file
    resp = ingest_manager.ingest_files([StagedFile(file_name, None)])
    logging.info(f"response from snowflake for file {file_name}: {resp['responseCode']}")


if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.CLIENT_SUPPORT_ORDERS_PIPE',
                                         private_key=private_key)
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append({
                "TXID": record["txid"],
                "RFID": record["rfid"],
                "CUSTOMER_ID": record["customer_id"],
                "PRODUCT_ID": record["product_id"],

                "ITEM": record["item"],
                "BAG_SIZE": record.get("bag_size"),
                "UNIT_PRICE": record.get("unit_price"),
                "QUANTITY": record.get("quantity"),
                "TOTAL_PRICE": record.get("total_price"),

                "ORIGIN_COUNTRY": record.get("origin_country"),
                "FAIR_TRADE_CERTIFIED": record.get("fair_trade_certified"),
                "ORGANIC_CERTIFIED": record.get("organic_certified"),

                "PURCHASE_TIME": record["purchase_time"],
                "SHIPPED_DATE": record.get("shipped_date"),
                "DELIVERED_DATE": record.get("delivered_date"),

                "REGION": record.get("region"),
                "NAME": record.get("name"),
                "STREET_ADDRESS": record.get("street_address"),
                "CITY": record.get("city"),
                "COUNTRY": record.get("country"),
                "POSTALCODE": record.get("postalcode"),
                "PHONE": record.get("phone"),
                "EMAIL": record.get("email"),

                "WAREHOUSE": record.get("warehouse"),
                "SHIPPING_METHOD": record.get("shipping_method"),
                "DELIVERY_STATUS": record.get("delivery_status"),
                "PAYMENT_METHOD": record.get("payment_method"),
                "PAYMENT_STATUS": record.get("payment_status"),

                "DELIVERY_DELAY_DAYS": record.get("delivery_delay_days"),
                "CARBON_SCORE": record.get("carbon_score"),
            })
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
        else:
            break    
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")