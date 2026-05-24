import streamlit as st
import pandas as pd
import sqlite3
import time

DB_PATH = "storage/retailstream.db"

st.set_page_config(
    page_title="RetailStream Live Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 RetailStream: Live Retail Analytics")
st.caption("Auto-refreshes every 5 seconds from Kafka stream via SQLite")

placeholder = st.empty()

while True:
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

        df = pd.read_sql("SELECT * FROM raw_events", con)
        anomaly_df = pd.read_sql("SELECT * FROM anomalies", con)
        con.close()

        total_revenue = df['revenue'].sum()
        total_events = len(df)
        high_value_count = df['is_high_value'].sum()
        anomaly_count = len(anomaly_df)

        by_category = df.groupby('product_category')['revenue'].sum().sort_values(ascending=False)
        by_store = df.groupby('store_id')['revenue'].sum().sort_values(ascending=False).head(5)
        by_hour = df.groupby('hour_of_day')['revenue'].sum()
        recent = df.sort_values('timestamp', ascending=False).head(10)[
            ['timestamp', 'store_id', 'product_category',
             'total_amount', 'payment_method', 'is_high_value']
        ]

        with placeholder.container():
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Revenue", f"${total_revenue:,.2f}")
            col2.metric("Total Events", f"{total_events:,}")
            col3.metric("High Value Transactions", f"{int(high_value_count):,}")
            col4.metric("Anomalies Detected", f"{int(anomaly_count):,}")

            st.divider()

            col5, col6 = st.columns(2)
            with col5:
                st.subheader("Revenue by Category")
                st.bar_chart(by_category)
            with col6:
                st.subheader("Top 5 Stores by Revenue")
                st.bar_chart(by_store)

            st.divider()

            col7, col8 = st.columns(2)
            with col7:
                st.subheader("Revenue by Hour")
                st.bar_chart(by_hour)
            with col8:
                st.subheader("Recent Transactions")
                st.dataframe(recent, use_container_width=True)

            st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")

    except Exception as e:
        with placeholder.container():
            st.info(f"Waiting for data... ({e})")

    time.sleep(5)