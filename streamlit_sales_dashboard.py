import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

session = get_active_session()


sns.set_palette(["#1f77b4","#d62728"])


query = """
SELECT *
FROM CLIENT_SUPPORT_ORDERS
"""
df = session.sql(query).to_pandas()

st.title("Client Orders & Sales Dashboard ")

st.subheader("💲Key Metrics💲")

total_orders = df.shape[0]
total_sales = df['TOTAL_PRICE'].sum()
unique_customers = df['CUSTOMER_ID'].nunique()
avg_delivery_delay = df['DELIVERY_DELAY_DAYS'].mean()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", total_orders)
col4.metric("Average Delivery Durartion (days)", f"{avg_delivery_delay:.2f}")
total_sales = df['TOTAL_PRICE'].sum()
total_orders = df.shape[0]
average_order_value = df['TOTAL_PRICE'].mean()
st.metric("Total Sales", f"${total_sales:,.2f}")
st.metric("Average Order Value", f"${average_order_value:,.2f}")


if "TOTAL_PRICE" in df.columns:
    st.header("📊 Total Sales Distribution")

    fig, ax = plt.subplots(figsize=(8,5))
    ax.hist(df["TOTAL_PRICE"], bins=20, color="#ff69b4", edgecolor='black')
    ax.set_xlabel("Total Price")
    ax.set_ylabel("Number of Orders")
    ax.set_title("Distribution of Sales Amount")
    st.pyplot(fig)


st.subheader("📈 Sales Over Time")

df['PURCHASE_TIME'] = pd.to_datetime(df['PURCHASE_TIME'])
sales_over_time = df.groupby(df['PURCHASE_TIME'].dt.date)['TOTAL_PRICE'].sum()
fig, ax = plt.subplots()
ax.plot(sales_over_time.index, sales_over_time.values, color='royalblue', marker='o')
ax.set_xlabel("Date")
ax.set_ylabel("Total Sales")
ax.set_title("Daily Sales")
plt.xticks(rotation=45)
st.pyplot(fig)


st.subheader("💰 Top Products by Sales")

top_products = df.groupby('ITEM')['TOTAL_PRICE'].sum().sort_values(ascending=False).head(10)
fig, ax = plt.subplots()
ax.barh(top_products.index[::-1], top_products.values[::-1], color='crimson')
ax.set_xlabel("Sales")
ax.set_title("Top 10 Products")
st.pyplot(fig)


st.subheader("📦 Sales by Bag Size")

sales_by_bag = df.groupby('BAG_SIZE')['TOTAL_PRICE'].sum().sort_values(ascending=False)
fig, ax = plt.subplots()
ax.bar(sales_by_bag.index, sales_by_bag.values, color='royalblue')
ax.set_ylabel("Sales")
ax.set_xlabel("Bag Size")
ax.set_title("Total Sales by Bag Size")
st.pyplot(fig)


st.subheader("🌍 Orders by Country")

orders_by_country = df['ORIGIN_COUNTRY'].value_counts().head(10)
fig, ax = plt.subplots()
ax.bar(orders_by_country.index, orders_by_country.values, color='hotpink')
ax.set_ylabel("Number of Orders")
ax.set_title("Top 10 Countries")
plt.xticks(rotation=45)
st.pyplot(fig)


st.subheader("🌐 Orders by Region")

orders_by_region = df['REGION'].value_counts()
fig, ax = plt.subplots()
ax.pie(orders_by_region.values, labels=orders_by_region.index, autopct='%1.1f%%', colors=['royalblue','hotpink','crimson'])
ax.set_title("Orders Distribution by Region")
st.pyplot(fig)

st.subheader("💵 Payment Methods Distribution")

payment_counts = df['PAYMENT_METHOD'].value_counts()
fig, ax = plt.subplots()
ax.bar(payment_counts.index, payment_counts.values, color='royalblue')
ax.set_ylabel("Number of Orders")
ax.set_title("Payment Methods")
plt.xticks(rotation=45)
st.pyplot(fig)

st.subheader("🧾 Orders by Payment Status")

payment_status_counts = df['PAYMENT_STATUS'].value_counts()
fig, ax = plt.subplots()
ax.pie(payment_status_counts.values, labels=payment_status_counts.index,
       autopct='%1.1f%%', colors=['royalblue','hotpink','crimson'])
ax.set_title("Payment Status Distribution")
st.pyplot(fig)


if "SHIPPING_METHOD" in df.columns:
    st.header("📦 Shipping Methods")

    shipping_counts = df["SHIPPING_METHOD"].value_counts()
    fig, ax = plt.subplots(figsize=(8,5))
    sns.barplot(x=shipping_counts.index, y=shipping_counts.values, palette=["#1f77b4", "#ff69b4", "#d62728"], ax=ax)
    ax.set_ylabel("Number of Orders")
    ax.set_xlabel("Shipping Method")
    ax.set_title("Orders by Shipping Method")
    st.pyplot(fig)

st.subheader("📅 Delivery Duration (Days)")

fig, ax = plt.subplots()
ax.hist(df['DELIVERY_DELAY_DAYS'].dropna(), bins=20, color='crimson', edgecolor='black')
ax.set_xlabel("Delivery Delay (Days)")
ax.set_ylabel("Number of Orders")
ax.set_title("Delivery Delay Distribution")
st.pyplot(fig)

st.subheader("✅ Delivery Status Overview")

delivery_status_counts = df['DELIVERY_STATUS'].value_counts()
fig, ax = plt.subplots()
ax.bar(delivery_status_counts.index, delivery_status_counts.values, color='crimson')
ax.set_ylabel("Number of Orders")
ax.set_title("Delivery Status")
plt.xticks(rotation=45)
st.pyplot(fig)


