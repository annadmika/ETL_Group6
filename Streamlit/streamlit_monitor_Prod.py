import os
import pandas as pd
import numpy as np
import altair as alt
import streamlit as st

# ---------- Page ----------
st.set_page_config(page_title="Drinkable • Pipeline Monitoring", page_icon="🧰", layout="wide")

# ---------- Theme / CSS ----------
CSS = """
<style>
.block-container { padding-top: 0.8rem; }
:root{
  --coffee:#7B4B2A;      /* brown */
  --leaf:#2E7D32;        /* green */
  --foam:#FFF7F0;        /* latte bg */
  --ink:#2E2A27;         /* text */
  --line:#E7D7C8;        /* soft border */
}
.hero {
  margin-top:-12px; padding:22px; border-radius:16px;
  background: linear-gradient(135deg, var(--foam) 0%, #F4E6DA 100%);
  border:1px solid var(--line); color:var(--ink);
}
.hero h1 {font-size:34px; margin:0 0 6px 0; line-height:1.1;}
.hero p {margin:0; color:#6B5F57;}
.badge {display:inline-block; padding:6px 12px; border-radius:999px;
  background:var(--leaf); color:#fff; font-weight:700; font-size:12px;}
.card {
  border-radius:16px; padding:16px; border:1px solid var(--line); background:#fff;
  box-shadow:0 2px 10px rgba(123,75,42,0.07);
}
.card h3 {font-size:12px; letter-spacing:.5px; color:#7b6e66;
  text-transform:uppercase; margin:0 0 10px 0;}
.kpi {font-size:26px; font-weight:800; color:var(--ink);}
.pass {color:#2E7D32; font-weight:700;}
.warn {color:#B68900; font-weight:700;}
.fail {color:#C62828; font-weight:700;}
.small {color:#7b6e66; font-size:12px;}
.section-title{margin:10px 0 4px 0;}
.spacer-24{height:24px;}
footer {visibility: hidden;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ---------- Snowflake connection ----------
try:
    from dotenv import load_dotenv
    load_dotenv()  # optional, for local .env
except Exception:
    pass

@st.cache_resource(show_spinner=False)
def sf_conn():
    import snowflake.connector
    cfg = st.secrets.get("snowflake", {})
    account   = cfg.get("account",   os.getenv("SNOWFLAKE_ACCOUNT"))
    user      = cfg.get("user",      os.getenv("SNOWFLAKE_USER"))
    password  = cfg.get("password",  os.getenv("SNOWFLAKE_PASSWORD"))
    role      = cfg.get("role",      os.getenv("SNOWFLAKE_ROLE"))
    warehouse = cfg.get("warehouse", os.getenv("SNOWFLAKE_WAREHOUSE"))
    database  = cfg.get("database",  os.getenv("SNOWFLAKE_DATABASE"))
    schema    = cfg.get("schema",    os.getenv("SNOWFLAKE_SCHEMA"))
    missing = [k for k,v in {
        "account":account,"user":user,"password":password,"role":role,
        "warehouse":warehouse,"database":database,"schema":schema
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing Snowflake config: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=account, user=user, password=password,
        role=role, warehouse=warehouse, database=database, schema=schema,
        client_session_keep_alive=True,
    )

def qdf(sql: str, params: dict | None = None) -> pd.DataFrame:
    cur = sf_conn().cursor()
    try:
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()

@st.cache_data(ttl=180, show_spinner=False)
def cached(sql: str, params: dict | None = None) -> pd.DataFrame:
    return qdf(sql, params)

# ---------- HERO ----------
st.markdown("""
<div class="hero">
  <span class="badge">Monitoring</span>
  <h1>Pipeline Health — Shopify → S3 → Snowflake</h1>
  <p>Freshness, volumes, data quality and schema drift on production tables.</p>
</div>
""", unsafe_allow_html=True)

# ---------- Sidebar filters ----------
st.sidebar.header("Filters")
lookback = st.sidebar.slider("Business lookback (days)", 7, 180, 60, step=1)

# ---------- FRESHNESS ----------
st.markdown("### ⏱️ Freshness & Throughput")

freshness_sql = """
with t as (
  select 'RAW'    as layer, 'shopify_orders_raw' as "table",
         count(*) as row_count, max(ingested_at) as last_load_utc
  from RAW.shopify_orders
  union all
  select 'SILVER','orders_silver', count(*), max(updated_at)
  from SILVER.orders
  union all
  select 'GOLD','sales_daily_gold', count(*), max(updated_at)
  from GOLD.sales_daily
)
select * from t
"""
freshness = cached(freshness_sql)

def lag_minutes(layer: str) -> float:
    ts = pd.to_datetime(freshness.loc[freshness["LAYER"]==layer, "LAST_LOAD_UTC"]).max()
    return (pd.Timestamp.utcnow() - ts).total_seconds()/60 if pd.notna(ts) else float("nan")

c1,c2,c3 = st.columns(3)
with c1:
    st.markdown(f'<div class="card"><h3>RAW Freshness</h3><div class="kpi">{lag_minutes("RAW"):.0f} min</div><div class="small">minutes since last load</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="card"><h3>SILVER Freshness</h3><div class="kpi">{lag_minutes("SILVER"):.0f} min</div><div class="small">minutes since last transform</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="card"><h3>GOLD Freshness</h3><div class="kpi">{lag_minutes("GOLD"):.0f} min</div><div class="small">minutes since last aggregate</div></div>', unsafe_allow_html=True)

st.markdown('<div class="spacer-24"></div>', unsafe_allow_html=True)
st.dataframe(freshness, use_container_width=True, hide_index=True)

# ---------- VOLUMES & DELTAS ----------
st.markdown("### 📦 Volume Deltas vs 7-day Avg (Orders)")

daily_sql = """
select
  date(order_date) as date,
  count(*)         as orders,
  sum(order_total) as revenue,
  count_if(status in ('delivered','shipped')) as success
from GOLD.sales_orders
where order_date >= dateadd(day, -%(d)s, current_date())
group by 1
order by 1
"""
daily = cached(daily_sql, {"d": lookback})
daily["DATE"] = pd.to_datetime(daily["DATE"])
daily.rename(columns=str.lower, inplace=True)

# rolling average & delta
daily["orders_7d_avg"] = daily["orders"].rolling(7).mean()
daily["delta_orders"] = (daily["orders"] - daily["orders_7d_avg"]) / daily["orders_7d_avg"] * 100

orders_bar = alt.Chart(daily).mark_bar().encode(
    x=alt.X("date:T", title=None),
    y=alt.Y("orders:Q", title="Orders"),
    tooltip=["date","orders","orders_7d_avg","delta_orders"]
).properties(height=230)
avg_line = alt.Chart(daily).mark_line().encode(
    x="date:T", y=alt.Y("orders_7d_avg:Q", title="7-day avg")
)
st.altair_chart(alt.layer(orders_bar, avg_line), use_container_width=True)

drops = daily[daily["delta_orders"] < -30]
if len(drops):
    last = drops.iloc[-1]
    st.warning(f"Significant drop detected: {last['date'].date()} → {last['delta_orders']:.0f}% vs 7-day avg.")
else:
    st.success("No abnormal order volume drops (>30%) vs 7-day avg in the window.")

# ---------- DATA QUALITY ----------
st.markdown("### ✅ Data Quality Checks")

dq_rows = []

# Null % country
r = cached("""select round(100*count_if(country is null)/nullif(count(*),0),2) as null_pct
              from GOLD.sales_orders""")
null_country = float(r.iloc[0,0])
dq_rows.append({
    "check":"Null rate — country (GOLD)",
    "value": f"{null_country:.2f}%",
    "status": "PASS" if null_country < 1 else ("WARN" if null_country < 3 else "FAIL")
})

# Duplicate order_id
r = cached("""select count(*) as dup_cnt from (
                select order_id from GOLD.sales_orders
                group by 1 having count(*)>1
              )""")
dup_cnt = int(r.iloc[0,0])
dq_rows.append({
    "check":"Duplicate orders (GOLD)",
    "value": dup_cnt,
    "status": "PASS" if dup_cnt==0 else "FAIL"
})

# Orphan items
r = cached("""select count(*) as orphan_items
              from GOLD.sales_order_items i
              left join GOLD.sales_orders o on o.order_id=i.order_id
              where o.order_id is null""")
orphans = int(r.iloc[0,0])
dq_rows.append({
    "check":"Orphan order_items",
    "value": orphans,
    "status": "PASS" if orphans==0 else "FAIL"
})

dq = pd.DataFrame(dq_rows)
def badge(s): return f'<span class="{("pass" if s=="PASS" else "warn" if s=="WARN" else "fail")}">{s}</span>'
dq["result"] = dq["status"].apply(badge)
st.write(dq[["check","value","status"]].to_html(escape=False, index=False), unsafe_allow_html=True)

# ---------- SCHEMA DRIFT ----------
st.markdown("### 🧬 Schema Drift — GOLD.SALES_ORDERS")

# Expected schema (adjust to your contract)
expected = pd.DataFrame({
    "column":["ORDER_ID","ORDER_DATE","COUNTRY","ORDER_TOTAL","STATUS","CUSTOMER_ID"],
    "dtype" :["NUMBER","DATE","TEXT","NUMBER","TEXT","NUMBER"],
    "nullable":[False, False, True, False, False, False]
})

actual = cached("""
select upper(column_name) as column,
       upper(data_type)   as dtype,
       case when is_nullable='YES' then true else false end as nullable
from GOLD.information_schema.columns
where table_name='SALES_ORDERS'
order by ordinal_position
""")

left, right = st.columns(2)
with left:
    st.caption("Expected")
    st.dataframe(expected, use_container_width=True, hide_index=True)
with right:
    st.caption("Actual")
    st.dataframe(actual, use_container_width=True, hide_index=True)

added = sorted(set(actual["COLUMN"]) - set(expected["column"]))
merged = actual.merge(expected, left_on="COLUMN", right_on="column", how="left", suffixes=("_actual","_expected"))
mismatch = merged[(merged["dtype_actual"]!=merged["dtype_expected"]) | (merged["nullable_actual"]!=merged["nullable_expected"])]

if added or len(mismatch):
    parts = []
    if added: parts.append("New columns: " + ", ".join(added))
    if len(mismatch): parts.append("Type/nullable mismatches on: " + ", ".join(mismatch["COLUMN"].tolist()))
    st.warning("Schema drift detected — " + " | ".join(parts))
else:
    st.success("No schema drift detected.")

# ---------- Footer note ----------
st.caption("Connected to Snowflake • role/warehouse/db/schema taken from secrets or env vars.")
