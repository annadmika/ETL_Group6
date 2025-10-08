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

import logging, sys, traceback
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)


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
        database="ECO_COFFEE_DWH",
        schema="RAW",
        warehouse="PIPELINE_WH",
        session_parameters={'QUERY_TAG': 'py-snowpipe-orders'}
    )


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')

    pandas_df = pd.DataFrame(batch)
    if pandas_df.empty:
        logging.warning("Skipping save: empty batch")
        return

    # Coerce to DATE (no time) for the three date fields
    for col in ["PURCHASE_TIME", "SHIPPED_DATE", "DELIVERED_DATE"]:
        if col in pandas_df.columns:
            pandas_df[col] = pd.to_datetime(pandas_df[col], errors="coerce").dt.date

    # Prepare file path BEFORE any try/except so variables always exist
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"

    # Convert to Arrow
    try:
        arrow_table = pa.Table.from_pandas(pandas_df, preserve_index=False)
    except Exception:
        logging.exception("Arrow conversion failed")
        return

    # Write Parquet
    try:
        pq.write_table(
            arrow_table,
            out_path,
            use_dictionary=False,
            compression='SNAPPY'
        )
        logging.info(f"Wrote parquet {file_name} with {len(batch)} rows")
    except Exception:
        logging.exception("Parquet write failed")
        return

    # PUT to table stage for RAW_CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE
    try:
        snow.cursor().execute("PUT 'file://{0}' @ECO_COFFEE_DWH.RAW.%RAW_CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE".format(out_path))
        logging.info(f"PUT succeeded for {file_name}")
        os.unlink(out_path)
    except Exception:
        logging.exception("PUT to table stage failed (or unlink failed)")
        return

    # Trigger Snowpipe
    try:
        resp = ingest_manager.ingest_files([StagedFile(file_name, None)])
        logging.info(f"Ingest requested for {file_name}: {resp['responseCode']}")
    except Exception:
        logging.exception("Snowpipe ingest failed")
        return


if __name__ == "__main__":
    try:
        args = sys.argv[1:]
        batch_size = int(args[0])
        snow = connect_snow()
        batch = []
        temp_dir = tempfile.TemporaryDirectory()
        private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
        host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
        ingest_manager = SimpleIngestManager(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            host=host,
            user=os.getenv("SNOWFLAKE_USER"),
            pipe='ECO_COFFEE_DWH.RAW.CLIENT_SUPPORT_ORDERS_PIPE',
            private_key=private_key
        )
        processed = 0
        print("Starting Snowpipe orders ingest...", flush=True)
        for message in sys.stdin:
            if message == '\n':
                break
            rec = json.loads(message)
            processed += 1
            batch.append({
                "TXID": rec["txid"],
                "RFID": rec["rfid"],
                "CUSTOMER_ID": rec["customer_id"],
                "PRODUCT_ID": rec["product_id"],

                "ITEM": rec["item"],
                "BAG_SIZE": rec.get("bag_size"),
                "UNIT_PRICE": rec.get("unit_price"),
                "QUANTITY": rec.get("quantity"),
                "TOTAL_PRICE": rec.get("total_price"),

                "ORIGIN_COUNTRY": rec.get("origin_country"),
                "FAIR_TRADE_CERTIFIED": rec.get("fair_trade_certified"),
                "ORGANIC_CERTIFIED": rec.get("organic_certified"),

                "PURCHASE_TIME": rec["purchase_time"],
                "SHIPPED_DATE": rec.get("shipped_date"),
                "DELIVERED_DATE": rec.get("delivered_date"),

                "REGION": rec.get("region"),
                "NAME": rec.get("name"),
                "STREET_ADDRESS": rec.get("street_address"),
                "CITY": rec.get("city"),
                "COUNTRY": rec.get("country"),
                "POSTALCODE": rec.get("postalcode"),
                "PHONE": rec.get("phone"),
                "EMAIL": rec.get("email"),

                "WAREHOUSE": rec.get("warehouse"),
                "SHIPPING_METHOD": rec.get("shipping_method"),
                "DELIVERY_STATUS": rec.get("delivery_status"),
                "PAYMENT_METHOD": rec.get("payment_method"),
                "PAYMENT_STATUS": rec.get("payment_status"),

                "DELIVERY_DELAY_DAYS": rec.get("delivery_delay_days"),
                "CARBON_SCORE": rec.get("carbon_score"),
            })
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                print(f"Progress: {processed} records processed...", flush=True)
                batch = []
        if len(batch) > 0:
            save_to_snowflake(snow, batch, temp_dir, ingest_manager)
        print(f"Done. Total records processed: {processed}", flush=True)
    except Exception:
        logging.error("Fatal error:\n%s", traceback.format_exc())
    finally:
        try:
            temp_dir.cleanup()
        except Exception:
            pass
        try:
            snow.close()
        except Exception:
            pass
        logging.info("Ingest complete")