# README — Snowflake Data Pipeline Architecture Overview

This project implements an automated data ingestion and transformation pipeline in Snowflake, designed to handle client orders and carbon emissions data for sustainability analytics.
The pipeline uses Snowflake’s multi-layer architecture (RAW → SILVER → GOLD → STREAMLIT_APPS) combined with streams and tasks to ensure data moves seamlessly and cleanly through each stage.

## Pipeline Architecture : 

======================================================

1. RAW Layer — Data Ingestion

Purpose: Store unprocessed raw data exactly as it is ingested from the source.

- Tables:
    - RAW_CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE
    - RAW_CARBON_EMISSIONS_PY_SNOWPIPE

Data is loaded automatically via Snowpipe.
A Python script (snowpipe_orders.py) triggers Snowpipe when new source files are available in cloud storage (e.g., AWS S3, GCS, or Azure Blob).

- Streams:
    - RAW_CLIENT_SUPPORT_ORDERS_STREAM
    - RAW_CARBON_EMISSIONS_STREAM

    These streams track changes (new rows) in the RAW tables, allowing tasks to automatically process new data.

======================================================

2. SILVER Layer — Data Cleaning

Purpose: Clean and standardize the raw data for analytical use.

- Tables:
    - CLIENT_SUPPORT_ORDERS_CLEAN
    - CARBON_EMISSIONS_CLEAN

- Tasks: (Tasks are triggered automatically after their corresponding RAW streams detect new data)
    - task_clean_orders → merges new orders from RAW_CLIENT_SUPPORT_ORDERS_STREAM into CLIENT_SUPPORT_ORDERS_CLEAN
    - task_clean_emissions → merges new emissions data from RAW_CARBON_EMISSIONS_STREAM into CARBON_EMISSIONS_CLEAN

Silver layer cleaning includes:
- Removing duplicate transactions (TXID)
- Converting item names to proper case (INITCAP)
- Standardizing bag sizes to uppercase
- Nulling invalid or future timestamps
- Handling negative or null carbon emissions values

These tasks run immediately after new data ingestion, ensuring that SILVER tables are always up to date.

======================================================

3. GOLD Layer — Curated Data for Analytics

Purpose: Store finalized, cleaned data ready for reporting and dashboards.

- Tables:
    - GOLD_CLIENT_SUPPORT_ORDERS
    - GOLD_CARBON_EMISSIONS

- Tasks:
    - task_gold_orders
    - task_gold_emissions

These tasks are triggered after the corresponding SILVER streams detect changes, ensuring that GOLD tables always mirror the cleaned SILVER tables.

======================================================

4. STREAMLIT_APPS Database — Data Serving Layer

Purpose: Provide read-optimized copies of the GOLD tables for Streamlit dashboards and visualization tools.

- Tables:
    - CLIENT_SUPPORT_ORDERS
    - CARBON_EMISSIONS

Stored under: STREAMLIT_APPS.GOLD_COPY

- Tasks:
    - task_copy_gold_orders_to_streamlit
    - task_copy_gold_emissions_to_streamlit

These copy tasks ensure that the STREAMLIT_APPS database always reflects the latest GOLD-layer data, ready to be queried by Streamlit applications.

======================================================

### snowpipe.py — Snowpipe Trigger Script

The snowpipe.py script is used to automate ingestion into the RAW tables.

Purpose:
- Detect new source files (from S3, local upload, etc.)
- Call Snowflake’s REST API to trigger Snowpipe ingestion

Typical Workflow:
- The script authenticates to Snowflake using credentials (user/role/key).
- It identifies new data files to load.
- It sends a REST API call to the Snowpipe endpoint:
        *https://<your_account>.snowflakecomputing.com/v1/data/pipes/<pipe_name>/insertFiles*
- Snowpipe loads the data into the corresponding RAW (Bronze layer) table.
- The RAW → SILVER → GOLD → STREAMLIT_APPS pipeline runs automatically via streams and tasks.

#### Example Command :
*python py_snowpipe_orders.py --pipe RAW_CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE --file_path ./data/orders_batch_2025-10-08.csv*

The script can also be run on a schedule (e.g., cron, Airflow, or CI/CD) to continuously feed data into Snowflake.


======================================================

### End-to-End Flow Summary

**| Source Files (CSV / JSON) |**

        V

*py_snowpipe.py (Triggers Snowpipe)*

        V

**| RAW Tables |**

        V

*(Tracked by Streams)*

        V

**| SILVER Tables (Cleaned Data) |**

        V

*(Tracked by Streams)*

        V

**| GOLD Tables |**

        V

**| STREAMLIT_APPS DB (Dashboard Layer) |**


======================================================


### Key Benefits :
- Fully automated, event-driven data pipeline
- Modular multi-layer architecture (RAW → SILVER → GOLD → STREAMLIT_APPS)
- Data quality guaranteed through cleaning logic
- Near real-time updates for analytics and dashboards
- Minimal manual intervention — once snowpipe.py runs, everything else flows automatically