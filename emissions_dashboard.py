# IMPORT DEPENDENCIES
import snowflake.connector
import pandas as pd
import streamlit as st
import plotly.express as px


# CONNECT TO SNOWFLAKE AND IMPORT CARBON EMISSIONS DATA FOR STREAMLIT DASHBOARD
conn = snowflake.connector.connect(
    user='AMIKA', # change to your user
    password='ETLwarehousing1', # change to your pw
    account='fcvvvvl-zib75701', # the group account identifier
    warehouse='ANALYTICS_WH',
    database='STREAMLIT_APPS',
    schema='INGEST_COPY',
    role='ACCOUNTADMIN'
)

query = "SELECT * FROM STREAMLIT_APPS.INGEST_COPY.CARBON_EMISSIONS"
df = pd.read_sql(query, conn)


# ---------------- Streamlit layout ----------------
st.set_page_config(page_title="Coffee Carbon Emissions Dashboard", layout="wide")
st.title("☕ Coffee Carbon Emissions Dashboard 🌱")

# Sidebar filters
warehouse_filter = st.sidebar.multiselect(
    "Select Warehouse", df['WAREHOUSE_NAME'].unique(), default=df['WAREHOUSE_NAME'].unique()
)
distance_filter = st.sidebar.multiselect(
    "Select Distance Class", df['DISTANCE_CLASS'].unique(), default=df['DISTANCE_CLASS'].unique()
)

# Filtered data
filtered_df = df[
    (df['WAREHOUSE_NAME'].isin(warehouse_filter)) &
    (df['DISTANCE_CLASS'].isin(distance_filter))
]

st.subheader("Filtered Data")
st.dataframe(filtered_df)

# ---------------- KPIs ----------------
st.subheader("Key Metrics")
total_emissions = filtered_df['ESTIMATED_EMISSIONS_KGCO2E'].sum()
avg_emissions_per_shipment = (filtered_df['ESTIMATED_EMISSIONS_KGCO2E'] / filtered_df['SHIPMENTS_COUNT']).mean()

col1, col2 = st.columns(2)
col1.metric("Total Emissions (kg CO2e)", f"{total_emissions:,.2f}")
col2.metric("Avg Emissions per Shipment (kg CO2e)", f"{avg_emissions_per_shipment:,.2f}")

# ---------------- Visualizations ----------------

# 1. Total Emissions Over Time
st.subheader("Total Emissions Over Time")
emissions_over_time = filtered_df.groupby('REPORTING_MONTH')['ESTIMATED_EMISSIONS_KGCO2E'].sum().reset_index()
fig1 = px.line(
    emissions_over_time,
    x='REPORTING_MONTH',
    y='ESTIMATED_EMISSIONS_KGCO2E',
    title='Total Emissions Over Time',
    labels={'ESTIMATED_EMISSIONS_KGCO2E': 'Estimated Emissions (kg CO2e)', 'REPORTING_MONTH': 'Reporting Month'}
)
st.plotly_chart(fig1, use_container_width=True)

# 2. Emissions by Warehouse
st.subheader("Emissions by Warehouse")
warehouse_emissions = filtered_df.groupby('WAREHOUSE_NAME')['ESTIMATED_EMISSIONS_KGCO2E'].sum().reset_index()
fig2 = px.bar(
    warehouse_emissions,
    x='WAREHOUSE_NAME',
    y='ESTIMATED_EMISSIONS_KGCO2E',
    title='Emissions by Warehouse',
    color='WAREHOUSE_NAME',
    labels={'ESTIMATED_EMISSIONS_KGCO2E': 'Estimated Emissions (kg CO2e)', 'WAREHOUSE_NAME': 'Warehouse'}
)
st.plotly_chart(fig2, use_container_width=True)

# 3. Emissions per kg by Shipping Method
st.subheader("Emissions per kg by Shipping Method")
filtered_df['EMISSIONS_PER_KG'] = filtered_df['ESTIMATED_EMISSIONS_KGCO2E'] / filtered_df['AVG_BATCH_SIZE_KG']

fig3 = px.bar(
    filtered_df,
    x='SHIPPING_METHOD',
    y='EMISSIONS_PER_KG',
    color='WAREHOUSE_NAME',
    title='Emissions per kg by Shipping Method',
    labels={
        'EMISSIONS_PER_KG': 'Emissions per kg',
        'SHIPPING_METHOD': 'Shipping Method',
        'WAREHOUSE_NAME': 'Warehouse'
    }
)
fig3.update_layout(barmode='stack')
st.plotly_chart(fig3, use_container_width=True)


# 4. Emissions by Origin Country (Map)
st.subheader("Emissions by Coffee Origin Country")
origin_emissions = filtered_df.groupby('ORIGIN_COUNTRY')['ESTIMATED_EMISSIONS_KGCO2E'].sum().reset_index()
fig4 = px.choropleth(
    origin_emissions,
    locations='ORIGIN_COUNTRY',
    locationmode='country names',
    color='ESTIMATED_EMISSIONS_KGCO2E',
    color_continuous_scale='Greens',
    title='Emissions by Coffee Origin Country',
    labels={'ESTIMATED_EMISSIONS_KGCO2E': 'Estimated Emissions (kg CO2e)', 'ORIGIN_COUNTRY': 'Origin Country'}
)
st.plotly_chart(fig4, use_container_width=True)

# 5. Distance Class Breakdown
st.subheader("Emissions by Distance Class")
distance_emissions = filtered_df.groupby('DISTANCE_CLASS')['ESTIMATED_EMISSIONS_KGCO2E'].sum().reset_index()
fig5 = px.pie(
    distance_emissions,
    names='DISTANCE_CLASS',
    values='ESTIMATED_EMISSIONS_KGCO2E',
    title='Emissions by Distance Class',
    labels={'ESTIMATED_EMISSIONS_KGCO2E': 'Estimated Emissions (kg CO2e)', 'DISTANCE_CLASS': 'Distance Class'}
)
st.plotly_chart(fig5, use_container_width=True)
