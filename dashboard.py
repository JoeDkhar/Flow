"""
dashboard.py
Streamlit dashboard for the Flow B2B Order Fulfillment analytics project.
Work in progress — KPI row only for now.
"""

import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent / "operations.db"

st.set_page_config(page_title="Flow — Order Analytics", layout="wide")
st.title("Flow — B2B Wholesale Order Fulfillment")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Pull the gold fact table from SQLite."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM fact_order_performance", conn)


df = load_data()

if df.empty:
    st.error("No data found. Run the ETL pipeline first.")
    st.stop()

# ── KPI row ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
active = df[df["was_cancelled"] == 0]

k1.metric("Total Orders",        f"{len(df):,}")
k2.metric("On-Time Rate",        f"{(active['sla_status']=='On-Time').mean()*100:.1f}%")
k3.metric("Backorder Rate",      f"{df['was_backordered'].mean()*100:.1f}%")
k4.metric("Avg Cycle Time (hrs)",f"{active['time_to_deliver_hours'].mean():.0f}h")

st.info("Charts coming in the next commit.")
