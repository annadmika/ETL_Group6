# streamlit_monitor_SNOWFLAKE_COFFEE_FULL.py
import pandas as pd
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
\
# ======================== PAGE / THEME ========================
st.set_page_config(page_title="☕ ECO COFFEE DWH — Monitoring", page_icon="🧰", layout="wide")
st.markdown("""
<style>
.block-container { padding-top: .8rem; }
:root{ --coffee:#7B4B2A; --leaf:#2E7D32; --foam:#FFF7F0; --ink:#2E2A27; --line:#E7D7C8; }
.hero { margin-top:36px; padding:20px; border-radius:16px; background:#654321;
        border:1px solid var(--line); color:#fff;}
.hero h1{font-size:30px;margin:0 0 6px 0}
.badge{display:inline-block;padding:6px 12px;border-radius:999px;background:var(--leaf);color:#fff;font-weight:700;font-size:12px;}
.card{border-radius:16px;padding:16px;margin-top:14px;border:1px solid var(--line);background:#fff;box-shadow:0 2px 10px rgba(123,75,42,.07);margin-bottom:8px}
.card h3{font-size:12px;letter-spacing:.5px;color:#7b6e66;text-transform:uppercase;margin:0 0 10px 0}
.kpi{font-size:26px;font-weight:800;color:#2E2A27}
.pass{color:#2E7D32;font-weight:700}
.warn{color:#B68900;font-weight:700}
.fail{color:#C62828;font-weight:700}
.small{color:#7b6e66;font-size:12px}
footer{visibility:hidden}
</style>
""", unsafe_allow_html=True)

# ======================== SNOWPARK SESSION ========================
@st.cache_resource
def session():
    return get_active_session()

@st.cache_data(show_spinner=False, ttl=60)
def read_sql(sql: str):
    return session().sql(sql).to_pandas()

# ======================== CONFIG ========================
DB = "ECO_COFFEE_DWH"
RAW = "RAW"
SILVER = "SILVER"
GOLD = "GOLD"

RAW_TABLES = [
    f"{DB}.{RAW}.RAW_CARBON_EMISSIONS_PY_SNOWPIPE",
    f"{DB}.{RAW}.RAW_CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE"
]
SILVER_TABLES = [
    f"{DB}.{SILVER}.CARBON_EMISSIONS_CLEAN",
    f"{DB}.{SILVER}.CLIENT_SUPPORT_ORDERS_CLEAN"
]
GOLD_TABLES = [
    f"{DB}.{GOLD}.GOLD_CARBON_EMISSIONS",
    f"{DB}.{GOLD}.GOLD_CLIENT_SUPPORT_ORDERS"
]

RAW_PIPES = [
    f"{DB}.{RAW}.RAW_CARBON_EMISSIONS_PIPE",
    f"{DB}.{RAW}.RAW_CLIENT_SUPPORT_ORDERS_PIPE"
]

# ======================== HERO ========================
st.markdown("""
<div class="hero">
  <span class="badge">Monitoring</span>
  <h1>☕ ECO COFFEE DWH — RAW → SILVER → GOLD</h1>
  <p>Freshness, Snowpipe throughput, query errors, tasks, and data quality.</p>
</div>
""", unsafe_allow_html=True)

# ======================== FILTERS ========================
st.sidebar.header("Filters")
lookback_h = st.sidebar.slider("Lookback window (hours)", 1, 168, 24)

# ======================== CONTEXT ========================
ctx = read_sql("""
SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_REGION() AS REGION, CURRENT_WAREHOUSE() AS WH,
       CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCH
""")
c1, c2, c3 = st.columns(3)
with c1: st.markdown(f'<div class="card"><h3>Account</h3><div class="kpi">{ctx.ACCOUNT[0]}</div></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="card"><h3>Warehouse</h3><div class="kpi">{ctx.WH[0]}</div></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="card"><h3>Role</h3><div class="kpi">{ctx.ROLE[0]}</div></div>', unsafe_allow_html=True)

# ======================== HELPERS ========================
def lag_minutes_utc(series):
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    now = pd.Timestamp.now(tz="UTC")
    return (now - ts).dt.total_seconds() / 60

def table_freshness(tbl):
    for col in ["DELIVERED_DATE","CREATED_AT","DATE","REPORTING_MONTH","ORDER_DATE"]:
        try:
            df = read_sql(f'SELECT COUNT(*) AS N, MAX("{col}") AS D FROM {tbl}')
            return int(df.N[0]), df.D[0]
        except:
            continue
    df = read_sql(f"SELECT COUNT(*) AS N FROM {tbl}")
    return int(df.N[0]), None

def section_fresh(name, tables):
    rows=[]
    for t in tables:
        n,d = table_freshness(t)
        rows.append({"layer":name,"table":t,"row_count":n,"last_load_time":d})
    df = pd.DataFrame(rows)
    df["lag_min"]=lag_minutes_utc(df["last_load_time"])
    return df

# ======================== A) FRESHNESS ========================
st.markdown("### ⏱️ Freshness by Layer")
fresh_all = pd.concat([
    section_fresh("RAW", RAW_TABLES),
    section_fresh("SILVER", SILVER_TABLES),
    section_fresh("GOLD", GOLD_TABLES)
], ignore_index=True)
st.dataframe(fresh_all, use_container_width=True, hide_index=True)

''' 
### THESE TILES WORKED ON STREAMLIT LOCALLY BUT NOT WITHIN SNOWFLAKE'S INCORPORATED STREAMLIT APP

# ======================== C) SNOWPIPE / COPY ========================
st.markdown("### 🚚 Snowpipe & COPY — throughput & errors")
try:
    pipes_usage = read_sql(f"""
    SELECT PIPE_NAME, START_TIME, BYTES_INSERTED, FILES_INSERTED, ERROR_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
    WHERE START_TIME >= DATEADD(hour, -{lookback_h}, CURRENT_TIMESTAMP())
      AND PIPE_NAME IN ({",".join([f"'{p}'" for p in RAW_PIPES])})
    ORDER BY START_TIME DESC
    """)
    st.dataframe(pipes_usage, use_container_width=True)
except Exception:
    st.info("ℹ️ No access to SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY for this role.")

# COPY errors via INFORMATION_SCHEMA
rows_err = []
for t in RAW_TABLES + SILVER_TABLES:
    try:
        db, sch, tbl, dbu, schu, tblu = split_full_name(t)
        df = read_sql(f"""
        SELECT FILE_NAME, LAST_LOAD_TIME, ROW_COUNT, FIRST_ERROR_MESSAGE
        FROM TABLE({db}.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => '{dbu}.{schu}.{tblu}',
            START_TIME => DATEADD(hour, -{lookback_h}, CURRENT_TIMESTAMP())
        ))
        WHERE FIRST_ERROR_MESSAGE IS NOT NULL
        ORDER BY LAST_LOAD_TIME DESC
        """)
        if not df.empty:
            df.insert(0, "TABLE_FQN", f"{dbu}.{schu}.{tblu}")
            rows_err.append(df)
    except Exception:
        pass
copy_err = pd.concat(rows_err, ignore_index=True) if rows_err else pd.DataFrame()
if copy_err.empty:
    st.success("No COPY errors found in the window (via INFORMATION_SCHEMA).")
else:
    st.dataframe(copy_err, use_container_width=True)

# ======================== D) WAREHOUSE LOAD ========================
st.markdown("### 🏭 Warehouse Load (Queue/Blocked/Running)")
try:
    wh_load = read_sql(f"""
    SELECT TO_TIMESTAMPNTZ(TIME_RANGE_START) AS TS, WAREHOUSE_NAME,
           AVG(QUEUED_LOAD_PERCENT) AS QUEUED, AVG(BLOCKED_PERCENT) AS BLOCKED, AVG(RUNNING) AS RUNNING
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE TIME_RANGE_START >= DATEADD(hour, -{lookback_h}, CURRENT_TIMESTAMP())
      AND WAREHOUSE_NAME = '{ctx.WH[0]}'
    GROUP BY 1,2
    ORDER BY 1
    """)
    if len(wh_load):
        chart = alt.Chart(wh_load).transform_fold(
            ["QUEUED","BLOCKED","RUNNING"], as_=["metric","value"]
        ).mark_line().encode(
            x=alt.X("TS:T", title=None),
            y=alt.Y("value:Q", title="%", stack=None),
            color="metric:N",
            tooltip=["TS","metric","value"]
        ).properties(height=240)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No warehouse load history in this window.")
except Exception:
    st.info("ℹ️ No access to SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY.")
# =================================================================================
'''

# ======================== D) QUERY HISTORY ERRORS ========================
st.markdown("### 🧾 Query Errors")
try:
    qry = read_sql(f"""
        SELECT QUERY_ID, USER_NAME, ERROR_CODE, ERROR_MESSAGE,
               START_TIME, TOTAL_ELAPSED_TIME/1000 AS SECS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(hour, -{lookback_h}, CURRENT_TIMESTAMP())
          AND ERROR_CODE IS NOT NULL
        ORDER BY START_TIME DESC
    """)
    if len(qry): st.dataframe(qry, use_container_width=True, hide_index=True)
    else: st.success("✅ No query errors found in this window.")
except:
    st.info("ℹ️ No access to ACCOUNT_USAGE.QUERY_HISTORY.")

# ======================== E) TASKS ========================
st.markdown("### ⏳ Task Monitoring")

try:
    tasks_df = read_sql(f"""
        SELECT 
            NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            STATE,
            COMPLETED_TIME,
            DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_SEC,
            ERROR_CODE,
            ERROR_MESSAGE
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE COMPLETED_TIME >= DATEADD(hour, -{lookback_h}, CURRENT_TIMESTAMP())
        ORDER BY COMPLETED_TIME DESC
    """)

    if len(tasks_df) == 0:
        st.info("✅ No task executions found in this window.")
    else:
        # --- Summary cards
        total_tasks = len(tasks_df)
        failed_tasks = (tasks_df["STATE"] == "FAILED").sum()
        succeeded_tasks = (tasks_df["STATE"] == "SUCCEEDED").sum()

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(
                f'<div class="card"><h3>Total runs</h3><div class="kpi">{total_tasks}</div></div>',
                unsafe_allow_html=True
            )
        with c2:
            st.markdown(
                f'<div class="card"><h3>✅ Succeeded</h3><div class="kpi pass">{succeeded_tasks}</div></div>',
                unsafe_allow_html=True
            )
        with c3:
            st.markdown(
                f'<div class="card"><h3>❌ Failed</h3><div class="kpi fail">{failed_tasks}</div></div>',
                unsafe_allow_html=True
            )

        # --- Status bar chart
        status_counts = tasks_df["STATE"].value_counts().reset_index()
        status_counts.columns = ["STATE", "COUNT"]
        chart_status = (
            alt.Chart(status_counts)
            .mark_bar()
            .encode(
                x=alt.X("STATE:N", title=None),
                y=alt.Y("COUNT:Q", title="Runs"),
                color=alt.Color("STATE:N", legend=None)
            )
            .properties(height=200, title="Task Execution Status")
        )
        st.altair_chart(chart_status, use_container_width=True)

        # --- Duration boxplot
        st.markdown("#### ⏱️ Duration by Task")
        # Truncate long task names if necessary
        tasks_df["TASK_LABEL"] = tasks_df["NAME"].apply(
            lambda x: x if len(x) <= 30 else x[:27] + "..."
        )

        chart_dur = (
            alt.Chart(tasks_df)
            .mark_boxplot(size=20)
            .encode(
                x=alt.X(
                    "TASK_LABEL:N",
                    title="Task Name",
                    axis=alt.Axis(labelAngle=-90, labelFontSize=7)
                ),
                y=alt.Y("DURATION_SEC:Q", title="Duration (s)"),
                color="STATE:N",
                tooltip=["NAME", "STATE", "DURATION_SEC", "COMPLETED_TIME"]
            )
            .properties(height=350)
        )
        st.altair_chart(chart_dur, use_container_width=True)



        # --- Failed task table
        failed_df = tasks_df[tasks_df["STATE"] == "FAILED"][
            ["DATABASE_NAME", "SCHEMA_NAME", "NAME", "ERROR_CODE", "ERROR_MESSAGE", "COMPLETED_TIME"]
        ]
        if len(failed_df):
            st.markdown("#### ⚠️ Failed Task Runs")
            st.dataframe(
                failed_df.rename(
                    columns={
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "NAME": "Task Name",
                        "ERROR_CODE": "Error Code",
                        "ERROR_MESSAGE": "Error Message",
                        "COMPLETED_TIME": "Completed Time"
                    }
                ),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("No failed tasks detected.")

except Exception as e:
    st.info(f"ℹ️ No access to SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY — {e}")



# ======================== F) DATA QUALITY ========================
st.markdown("### ✅ Data Quality Checks")

dq_rows = []

# ---- ORDERS QUALITY CHECKS ----
try:
    df_orders = read_sql(f"""
        SELECT 
            COUNT(*) AS N,
            SUM(IFF("CUSTOMER_ID" IS NULL, 1, 0)) AS N_NULL_CUSTOMER,
            SUM(IFF("TXID" IS NULL, 1, 0)) AS N_NULL_TXID,
            COUNT(DISTINCT "TXID") AS N_DISTINCT_TXID,
            SUM(IFF("PURCHASE_TIME" > CURRENT_TIMESTAMP(), 1, 0)) AS N_FUTURE_PURCHASES
        FROM {DB}.{SILVER}.CLIENT_SUPPORT_ORDERS_CLEAN
    """)

    total = df_orders["N"][0] or 1

    null_customer_rate = df_orders["N_NULL_CUSTOMER"][0] / total * 100
    null_txid_rate = df_orders["N_NULL_TXID"][0] / total * 100
    duplicate_txid = total - df_orders["N_DISTINCT_TXID"][0]
    future_purchases = df_orders["N_FUTURE_PURCHASES"][0]

    dq_rows += [
        {"check": "Null rate CUSTOMER_ID", "value": f"{null_customer_rate:.1f}%", "status": "PASS" if null_customer_rate < 5 else "FAIL"},
        {"check": "Null rate TXID", "value": f"{null_txid_rate:.1f}%", "status": "PASS" if null_txid_rate < 5 else "FAIL"},
        {"check": "Duplicate TXID count", "value": f"{duplicate_txid}", "status": "PASS" if duplicate_txid == 0 else "FAIL"},
        {"check": "Future PURCHASE_TIME", "value": f"{future_purchases}", "status": "PASS" if future_purchases == 0 else "FAIL"},
    ]

except Exception as e:
    st.warning(f"⚠️ Error checking orders data: {e}")

# ---- EMISSIONS QUALITY CHECKS ----
try:
    df_emissions = read_sql(f"""
        SELECT 
            COUNT(*) AS N,
            SUM(IFF("ESTIMATED_EMISSIONS_KGCO2E" < 0, 1, 0)) AS N_NEGATIVE,
            SUM(IFF("REPORTING_MONTH" > CURRENT_DATE(), 1, 0)) AS N_FUTURE_REPORTS
        FROM {DB}.{SILVER}.CARBON_EMISSIONS_CLEAN
    """)

    total_em = df_emissions["N"][0] or 1
    negative_emissions = df_emissions["N_NEGATIVE"][0]
    future_reports = df_emissions["N_FUTURE_REPORTS"][0]

    dq_rows += [
        {"check": "Negative ESTIMATED_EMISSIONS_KGCO2E", "value": f"{negative_emissions}", "status": "PASS" if negative_emissions == 0 else "FAIL"},
        {"check": "Future REPORTING_MONTH", "value": f"{future_reports}", "status": "PASS" if future_reports == 0 else "FAIL"},
    ]

except Exception as e:
    st.warning(f"⚠️ Error checking emissions data: {e}")

# ---- DISPLAY RESULTS ----
dq = pd.DataFrame(dq_rows) if dq_rows else pd.DataFrame([{"check": "(example)", "value": "—", "status": "PASS"}])
dq["result"] = dq["status"].apply(lambda s: f"<span class='{ 'pass' if s=='PASS' else 'fail'}'>{s}</span>")

st.write(dq[["check", "value", "result"]].to_html(escape=False, index=False), unsafe_allow_html=True)
