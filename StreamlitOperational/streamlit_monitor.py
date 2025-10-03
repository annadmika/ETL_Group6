# streamlit_monitor.py
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from datetime import datetime, timedelta

st.set_page_config(page_title="Pipeline Monitoring — Drinkable", page_icon="🧰", layout="wide")

# ----------------------- STYLES -----------------------
CSS = """
<style>
/* pull the page up a bit */
.block-container { padding-top: 0.8rem; }

/* brand palette */
:root{
  --coffee:#7B4B2A;      /* brown */
  --leaf:#2E7D32;        /* green */
  --foam:#FFF7F0;        /* latte background */
  --ink:#2E2A27;         /* text */
  --line:#E7D7C8;        /* soft border */
}

/* hero */
.hero {
  margin-top: 40px;     /* move up */
  padding: 22px;
  border-radius: 16px;
  background: linear-gradient(135deg, var(--foam) 0%, #F4E6DA 100%);
  border: 1px solid var(--line);
  color: var(--ink);
}
.hero h1 {font-size: 34px; margin: 0 0 6px 0; line-height:1.1;}
.hero p {margin: 0; color:#6B5F57;}
.badge {
  display:inline-block; padding:6px 12px; border-radius:999px;
  background: var(--leaf); color:#fff; font-weight:700; font-size:12px;
}

/* KPI cards */
.card {
  border-radius: 16px;
  padding: 16px;
  border: 1px solid var(--line);
  background: #FFFFFF;
  box-shadow: 0 2px 10px rgba(123,75,42,0.07);
  margin-bottom: 8px;
}
.card h3 {
  font-size: 12px; letter-spacing:.5px; color:#7b6e66;
  text-transform: uppercase; margin:0 0 10px 0;
}
.kpi {font-size:26px; font-weight:800; color: var(--ink);}

/* badge colors */
.pass {color:#2E7D32; font-weight:700;}
.warn {color:#B68900; font-weight:700;}
.fail {color:#C62828; font-weight:700;}
.small {color:#7b6e66; font-size:12px;}

/* utilities */
.section-title{margin:10px 0 4px 0;}
.spacer-16{height:16px;}
.spacer-24{height:24px;}
footer {visibility: hidden;}
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

# ----------------------- DEMO GENERATORS -----------------------
@st.cache_data
def make_demo_orders(days=120, seed=7):
    np.random.seed(seed)
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=days, freq="D")
    countries = ["FR","DE","ES","IT","UK","US"]
    rows = []
    oid = 10_000
    for d in dates:
        base = 38 + 8*np.sin((d.dayofyear%30)/30*2*np.pi)
        noise = np.random.normal(0,5)
        count = max(5, int(base + noise))
        if np.random.rand() < 0.03:
            count = int(count * np.random.uniform(0.2, 0.5))
        for _ in range(count):
            oid += 1
            total = np.random.lognormal(mean=3.25, sigma=0.35)
            status = np.random.choice(["delivered","shipped","cancelled","failed"], p=[0.82,0.1,0.05,0.03])
            rows.append([oid, d, np.random.choice(countries, p=[.32,.14,.14,.1,.2,.1]), round(total,2), status])
    df = pd.DataFrame(rows, columns=["order_id","order_date","country","order_total","status"])
    df["is_success"] = df["status"].isin(["delivered","shipped"])
    return df

@st.cache_data
def make_freshness_demo():
    now = pd.Timestamp.utcnow().floor("min")
    df = pd.DataFrame({
        "layer":["RAW","SILVER","GOLD"],
        "table":["shopify_orders_raw","orders_silver","sales_daily_gold"],
        "row_count":[420_000, 210_000, 120],
        "last_load_utc":[now - pd.Timedelta("70min"),
                         now - pd.Timedelta("28min"),
                         now - pd.Timedelta("9min")]
    })
    return df

@st.cache_data
def random_schema_drift():
    expected = pd.DataFrame({
        "column":["order_id","order_date","country","order_total","status","customer_id"],
        "dtype":["NUMBER","DATE","STRING","NUMBER","STRING","NUMBER"],
        "nullable":[False, False, False, False, False, False]
    })
    actual = expected.copy()
    actual.loc[actual["column"]=="status","dtype"] = np.random.choice(["STRING","NUMBER"], p=[.85,.15])
    actual.loc[actual["column"]=="customer_id","nullable"] = np.random.choice([False, True], p=[.9,.1])
    if np.random.rand() < 0.25:
        actual = pd.concat([actual, pd.DataFrame([{"column":"coupon_code","dtype":"STRING","nullable":True}])], ignore_index=True)
    return expected, actual

orders = make_demo_orders()
freshness = make_freshness_demo()

# ----------------------- HERO -----------------------
st.markdown("""
<div class="hero">
  <span class="badge">Monitoring</span>
  <h1>Pipeline Health — Shopify → S3 → Snowflake</h1>
  <p>Check freshness, volumes, data quality, drift, and anomalies. Demo data only.</p>
</div>
""", unsafe_allow_html=True)

# ----------------------- FILTERS -----------------------
st.sidebar.markdown("### Filters")
lookback = st.sidebar.slider("Business lookback (days)", 7, 120, 60, step=1)
cut = pd.Timestamp.today().normalize() - pd.Timedelta(days=lookback)

# ----------------------- SECTION: FRESHNESS -----------------------
st.markdown("### ⏱️ Freshness & Throughput")
c1,c2,c3 = st.columns(3)
def lag_min(layer):
    try:
        return (pd.Timestamp.utcnow() - pd.to_datetime(freshness.loc[freshness['layer']==layer,'last_load_utc']).max()).total_seconds()/60
    except Exception:
        return np.nan

with c1:
    st.markdown('<div class="card"><h3>RAW freshness</h3><div class="kpi">{:.0f} min</div><div class="small">minutes since last load</div></div>'.format(lag_min("RAW")), unsafe_allow_html=True)
with c2:
    st.markdown('<div class="card"><h3>SILVER freshness</h3><div class="kpi">{:.0f} min</div><div class="small">minutes since last transform</div></div>'.format(lag_min("SILVER")), unsafe_allow_html=True)
with c3:
    st.markdown('<div class="card"><h3>GOLD freshness</h3><div class="kpi">{:.0f} min</div><div class="small">minutes since last aggregate</div></div>'.format(lag_min("GOLD")), unsafe_allow_html=True)

st.dataframe(freshness, use_container_width=True, hide_index=True)

# ----------------------- SECTION: VOLUMES & DELTAS -----------------------
st.markdown("### 📦 Volume Deltas vs 7‑day Avg")
daily = (orders.groupby(orders["order_date"].dt.date)
         .agg(orders=("order_id","count"),
              revenue=("order_total","sum"),
              success=("is_success","sum"))
         .reset_index().rename(columns={"order_date":"date"}))
daily["date"] = pd.to_datetime(daily["date"])
daily = daily[daily["date"] >= cut].copy()

daily["orders_7d_avg"] = daily["orders"].rolling(7).mean()
daily["delta_orders"] = (daily["orders"] - daily["orders_7d_avg"]) / daily["orders_7d_avg"] * 100

orders_chart = alt.Chart(daily).mark_bar().encode(
    x=alt.X("date:T", title=None),
    y=alt.Y("orders:Q", title="Orders"),
    tooltip=["date","orders","orders_7d_avg","delta_orders"]
).properties(height=230)
avg_line = alt.Chart(daily).mark_line().encode(x="date:T", y=alt.Y("orders_7d_avg:Q", title="7d avg"))
st.altair_chart(alt.layer(orders_chart, avg_line), use_container_width=True)

alerts = daily[daily["delta_orders"] < -30][["date","orders","orders_7d_avg","delta_orders"]]
if len(alerts):
    st.warning(f"Significant drops detected on {len(alerts)} day(s). Latest example: {alerts.iloc[-1]['date'].date()} → {alerts.iloc[-1]['delta_orders']:.0f}% vs 7d avg.")
else:
    st.success("No abnormal volume drops vs 7‑day average.")

# ----------------------- SECTION: DATA QUALITY -----------------------
st.markdown("### ✅ Data Quality Checks (Demo)")
dq = pd.DataFrame([
    {"check":"Null rate — order_total (GOLD)", "value":"0.3%", "status":"PASS"},
    {"check":"Null rate — country (GOLD)", "value":"2.6%", "status":"WARN" if np.random.rand()<.7 else "PASS"},
    {"check":"Duplicate orders (GOLD)", "value":"0", "status":"PASS"},
    {"check":"Orphan order_items", "value":"12", "status":"FAIL" if np.random.rand()<.4 else "PASS"},
    {"check":"Negative totals", "value":"0", "status":"PASS"},
])
def badge(s):
    if s=="PASS": return '<span class="pass">PASS</span>'
    if s=="WARN": return '<span class="warn">WARN</span>'
    return '<span class="fail">FAIL</span>'
dq["result"] = dq["status"].apply(badge)
st.write(dq[["check","value","result"]].to_html(escape=False, index=False), unsafe_allow_html=True)

# ----------------------- SECTION: SCHEMA DRIFT -----------------------
st.markdown("### 🧬 Schema Drift — GOLD.sales_orders (expected vs actual)")
expected, actual = random_schema_drift()
left, right = st.columns(2)
with left:
    st.caption("Expected schema")
    st.dataframe(expected, hide_index=True, use_container_width=True)
with right:
    st.caption("Actual schema (latest load)")
    st.dataframe(actual, hide_index=True, use_container_width=True)

added_cols = set(actual["column"]) - set(expected["column"])
dtype_issues = actual.merge(expected, on="column", how="left", suffixes=("_actual","_expected"))
dtype_issues = dtype_issues[(dtype_issues["dtype_actual"]!=dtype_issues["dtype_expected"]) | (dtype_issues["nullable_actual"]!=dtype_issues["nullable_expected"])]
if len(added_cols) or len(dtype_issues):
    msg = []
    if added_cols: msg.append(f"New columns: {', '.join(sorted(added_cols))}")
    if len(dtype_issues): msg.append("Type/nullable mismatches on: " + ", ".join(dtype_issues["column"].tolist()))
    st.warning("Schema drift detected — " + " | ".join(msg))
else:
    st.success("No schema drift detected.")

# ----------------------- SECTION: SIMPLE ANOMALY (Z‑score) -----------------------
st.markdown("### 📉 Anomaly Detection (Revenue Z‑score)")
daily["rev_mean"] = daily["revenue"].rolling(14).mean()
daily["rev_std"]  = daily["revenue"].rolling(14).std()
daily["z"] = (daily["revenue"] - daily["rev_mean"]) / daily["rev_std"]
anom = daily[(daily["z"].abs() >= 2) & daily["rev_std"].notna()][["date","revenue","z"]]

chart = alt.Chart(daily).mark_line().encode(
    x="date:T",
    y=alt.Y("revenue:Q", title="Revenue (€)")
).properties(height=240)
points = alt.Chart(anom).mark_point(size=90).encode(
    x="date:T", y="revenue:Q", tooltip=["date","revenue","z"]
)
st.altair_chart(chart + points, use_container_width=True)

if len(anom):
    st.error(f"{len(anom)} revenue anomaly point(s) beyond ±2σ. Latest: {anom.iloc[-1]['date'].date()} (z={anom.iloc[-1]['z']:.2f}).")
else:
    st.success("No revenue anomalies beyond ±2σ in the selected window.")

# ----------------------- SECTION: RUN LOG (DEMO) -----------------------
st.markdown("### 🧾 Last 10 Loads (Job Log — Demo)")
log = pd.DataFrame({
    "ts":[(pd.Timestamp.utcnow()-pd.Timedelta(minutes=10*i)).strftime("%Y-%m-%d %H:%M") for i in range(10)],
    "job":["RAW→S3","SILVER transform","GOLD aggregates","GOLD aggregates","SILVER transform","RAW→S3","GOLD aggregates","SILVER transform","RAW→S3","GOLD aggregates"],
    "duration_s":[58, 120, 45, 41, 140, 60, 44, 150, 62, 46],
    "status":np.where(np.random.rand(10)<0.88, "SUCCESS", "FAILED")
})
st.dataframe(log, hide_index=True, use_container_width=True)
failed = log[log["status"]=="FAILED"]
if len(failed):
    st.error(f"{len(failed)} job(s) failed in the last 10 runs. Investigate and re‑run.")
else:
    st.success("All recent jobs succeeded.")
