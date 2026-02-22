"""
dashboard.py
Streamlit analytics dashboard for the Flow B2B Order Fulfillment project.

Run:
    streamlit run dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Flow — Order Analytics",
    page_icon="📦",
    layout="wide",
)

DB_PATH = Path(__file__).parent / "operations.db"


# ── Data loading ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Pull the gold fact table from SQLite and parse date columns."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM fact_order_performance", conn)
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


df = load_data()

if df.empty:
    st.error("No data found. Run `python scripts/etl_pipeline.py` first.")
    st.stop()


# ── Sidebar filters ────────────────────────────────────────────────────────

st.sidebar.header("Filters")

regions = st.sidebar.multiselect(
    "Region",
    options=sorted(df["region"].unique()),
    default=sorted(df["region"].unique()),
)

priorities = st.sidebar.multiselect(
    "Priority",
    options=["Critical", "Expedited", "Standard"],
    default=["Critical", "Expedited", "Standard"],
)

teams = st.sidebar.multiselect(
    "Fulfillment Team",
    options=sorted(df["fulfillment_team"].unique()),
    default=sorted(df["fulfillment_team"].unique()),
)

date_min = df["order_date"].dt.date.min()
date_max = df["order_date"].dt.date.max()
date_range = st.sidebar.date_input(
    "Order Date Range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

# Apply filters
mask = (
    df["region"].isin(regions)
    & df["priority"].isin(priorities)
    & df["fulfillment_team"].isin(teams)
    & df["order_date"].dt.date.between(*date_range)
)
filtered = df[mask].copy()
active   = filtered[filtered["was_cancelled"] == 0]


# ── Page header ────────────────────────────────────────────────────────────

st.title("📦 Flow — B2B Wholesale Order Fulfillment")
st.caption(
    f"Showing {len(filtered):,} of {len(df):,} orders "
    f"({len(active):,} active, {filtered['was_cancelled'].sum():,} cancelled)"
)
st.divider()


# ── KPI row ────────────────────────────────────────────────────────────────

k1, k2, k3, k4 = st.columns(4)

on_time_rate = (active["sla_status"] == "On-Time").mean() * 100
backorder_rate = filtered["was_backordered"].mean() * 100
avg_cycle = active["time_to_deliver_hours"].dropna().mean()

k1.metric("Total Orders",          f"{len(filtered):,}")
k2.metric("On-Time Delivery Rate", f"{on_time_rate:.1f}%")
k3.metric("Backorder Rate",        f"{backorder_rate:.1f}%")
k4.metric("Avg Cycle Time",        f"{avg_cycle:.0f} hrs")

st.divider()


# ── Chart grid ─────────────────────────────────────────────────────────────

col_left, col_right = st.columns(2)

# Chart 1: Avg time-to-ship by team (reveals Team C bottleneck)
with col_left:
    st.subheader("Avg Time-to-Ship by Fulfillment Team")
    ship_by_team = (
        active.groupby("fulfillment_team")["time_to_ship_hours"]
        .mean()
        .reset_index()
        .sort_values("time_to_ship_hours", ascending=True)
        .rename(columns={"time_to_ship_hours": "Avg Hours", "fulfillment_team": "Team"})
    )
    fig1 = px.bar(
        ship_by_team,
        x="Avg Hours",
        y="Team",
        orientation="h",
        color="Avg Hours",
        color_continuous_scale="Reds",
        labels={"Avg Hours": "Average Hours"},
    )
    fig1.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig1, use_container_width=True)

# Chart 2: Daily order volume
with col_right:
    st.subheader("Daily Order Volume")
    daily = (
        filtered.groupby(filtered["order_date"].dt.date)
        .size()
        .reset_index(name="Orders")
        .rename(columns={"order_date": "Date"})
    )
    fig2 = px.line(daily, x="Date", y="Orders", markers=False)
    fig2.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig2, use_container_width=True)

col_left2, col_right2 = st.columns(2)

# Chart 3: SLA breach vs on-time by priority
with col_left2:
    st.subheader("SLA Performance by Priority")
    sla_pivot = (
        active.groupby(["priority", "sla_status"])
        .size()
        .reset_index(name="Count")
    )
    fig3 = px.bar(
        sla_pivot,
        x="priority",
        y="Count",
        color="sla_status",
        barmode="stack",
        color_discrete_map={"On-Time": "#2ecc71", "Breached": "#e74c3c"},
        category_orders={"priority": ["Critical", "Expedited", "Standard"]},
        labels={"sla_status": "SLA Status", "priority": "Priority"},
    )
    fig3.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig3, use_container_width=True)

# Chart 4: Order volume by product category
with col_right2:
    st.subheader("Orders by Product Category")
    cat_counts = (
        filtered.groupby("product_category")
        .size()
        .reset_index(name="Orders")
        .rename(columns={"product_category": "Category"})
    )
    fig4 = px.pie(cat_counts, names="Category", values="Orders", hole=0.4)
    fig4.update_layout(margin=dict(l=0, r=0, t=0, b=40))
    st.plotly_chart(fig4, use_container_width=True)


# ── Data table ─────────────────────────────────────────────────────────────

st.divider()
st.subheader("Order Detail")

display_cols = [
    "order_id", "buyer_name", "region", "fulfillment_team",
    "priority", "time_to_ship_hours", "sla_status",
    "bottleneck_stage", "was_backordered",
]
st.dataframe(
    filtered[display_cols].rename(columns={
        "order_id":          "Order",
        "buyer_name":        "Buyer",
        "region":            "Region",
        "fulfillment_team":  "Team",
        "priority":          "Priority",
        "time_to_ship_hours":"Ship Time (h)",
        "sla_status":        "SLA",
        "bottleneck_stage":  "Bottleneck",
        "was_backordered":   "Backordered",
    }),
    use_container_width=True,
    hide_index=True,
)

st.download_button(
    label="Download filtered data as CSV",
    data=filtered[display_cols].to_csv(index=False),
    file_name="flow_filtered_orders.csv",
    mime="text/csv",
)
