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

    pandas_df = pd.DataFrame(batch)

    # Ensure DATE type for reporting_month
    if "REPORTING_MONTH" in pandas_df.columns:
        pandas_df["REPORTING_MONTH"] = pd.to_datetime(pandas_df["REPORTING_MONTH"], errors='coerce').dt.date

    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"

    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')

    # Put file into the table stage of CARBON_EMISSIONS_PY_SNOWPIPE
    snow.cursor().execute("PUT 'file://{0}' @%CARBON_EMISSIONS_PY_SNOWPIPE".format(out_path))
    os.unlink(out_path)

    # Trigger Snowpipe
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
                                        host=os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
                                        user=os.getenv("SNOWFLAKE_USER"),
                                        pipe='INGEST.INGEST.CARBON_EMISSIONS_PIPE',
                                        private_key=private_key        
)
    for message in sys.stdin:
        if message != '\n':
            rec = json.loads(message)

            batch.append({
                "RECORD_ID": rec["record_id"],
                "REPORTING_MONTH": rec["reporting_month"],
                "WAREHOUSE_ID": rec["warehouse_id"],
                "WAREHOUSE_NAME": rec["warehouse_name"],
                "WAREHOUSE_COUNTRY": rec["warehouse_country"],
                "ORIGIN_COUNTRY": rec["origin_country"],
                "DISTANCE_CLASS": rec["distance_class"],
                "SHIPPING_METHOD": rec["shipping_method"],
                "SHIPMENTS_COUNT": rec["shipments_count"],
                "AVG_BATCH_SIZE_KG": rec["avg_batch_size_kg"],
                "ESTIMATED_EMISSIONS_KGCO2E": rec["estimated_emissions_kgCO2e"],
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