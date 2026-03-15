"""
etl_pipeline.py
===============
Flow Wholesale Order Fulfillment Pipeline — ETL Runner

Steps
-----
1. Ingest  : Load raw CSVs into SQLite (operations.db) as raw_orders / raw_logs.
2. Transform: Execute sql_queries/transformations.sql to build fact_order_performance.
3. Validate : Assert data-quality rules; raise ValueError if any check fails.
4. Export  : Write outputs/gold_order_performance.csv and outputs/dashboard_data.json.

Run
---
    python scripts/etl_pipeline.py

Dependencies
------------
    sqlite3, pandas, json, logging, pathlib  (all stdlib / pandas)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths  (all resolved relative to this file so the script is location-agnostic)
# ---------------------------------------------------------------------------
REPO_ROOT: Path = Path(__file__).resolve().parents[1]

# Input data
RAW_ORDERS_CSV: Path = REPO_ROOT / "data" / "raw" / "orders.csv"
RAW_LOGS_CSV:   Path = REPO_ROOT / "data" / "raw" / "order_logs.csv"

# SQL transformation
TRANSFORM_SQL: Path = REPO_ROOT / "sql_queries" / "transformations.sql"

# Database
DB_PATH: Path = REPO_ROOT / "operations.db"

# Outputs
OUTPUTS_DIR:    Path = REPO_ROOT / "outputs"
GOLD_CSV_PATH:  Path = OUTPUTS_DIR / "gold_order_performance.csv"
JSON_PATH:      Path = OUTPUTS_DIR / "dashboard_data.json"

# Canonical domain values used in "filters" section of the JSON
REGIONS:     list[str] = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
PRIORITIES:  list[str] = ["Critical", "Expedited", "Standard"]
TEAMS:       list[str] = ["Team A", "Team B", "Team C", "Team D"]
CATEGORIES:  list[str] = ["Electronics", "Apparel", "Industrial", "Food & Bev", "Home Goods"]


# ---------------------------------------------------------------------------
# Step 1 · Ingest
# ---------------------------------------------------------------------------

def ingest(conn: sqlite3.Connection) -> None:
    """
    Load raw CSVs into SQLite, replacing existing raw tables on every run.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection to operations.db.

    Raises
    ------
    FileNotFoundError
        If either source CSV is missing from the expected location.
    """
    # Validate source files exist before attempting to read them
    for path in (RAW_ORDERS_CSV, RAW_LOGS_CSV):
        if not path.exists():
            raise FileNotFoundError(
                f"Required input file not found: {path}\n"
                f"Run `python scripts/generate_data.py` first to create the raw data."
            )

    log.info("Ingesting %s …", RAW_ORDERS_CSV.name)
    orders_df = pd.read_csv(RAW_ORDERS_CSV)
    orders_df.to_sql("raw_orders", conn, if_exists="replace", index=False)
    log.info("  ✓ raw_orders loaded  (%d rows)", len(orders_df))

    log.info("Ingesting %s …", RAW_LOGS_CSV.name)
    logs_df = pd.read_csv(RAW_LOGS_CSV)
    logs_df.to_sql("raw_logs", conn, if_exists="replace", index=False)
    log.info("  ✓ raw_logs  loaded   (%d rows)", len(logs_df))


# ---------------------------------------------------------------------------
# Step 2 · Transform
# ---------------------------------------------------------------------------

def transform(conn: sqlite3.Connection) -> None:
    """
    Execute the SQL transformation to build ``fact_order_performance``.

    Reads the CTE chain from ``sql_queries/transformations.sql`` and runs it
    inside a single ``executescript()`` call wrapped in explicit DROP / CREATE
    TABLE statements.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection to operations.db.

    Raises
    ------
    FileNotFoundError
        If the SQL file is missing.
    """
    if not TRANSFORM_SQL.exists():
        raise FileNotFoundError(
            f"Transformation SQL not found: {TRANSFORM_SQL}\n"
            f"Expected at: sql_queries/transformations.sql"
        )

    log.info("Reading transformation SQL from %s …", TRANSFORM_SQL.name)
    sql_body = TRANSFORM_SQL.read_text(encoding="utf-8").strip()

    # Build the full script: drop the old table then recreate it.
    # executescript() requires statements to end with semicolons.
    script = (
        "DROP TABLE IF EXISTS fact_order_performance;\n"
        f"CREATE TABLE fact_order_performance AS\n{sql_body};"
    )

    log.info("Executing transformation (DROP + CREATE TABLE) …")
    conn.executescript(script)
    conn.commit()

    # Quick row count confirmation
    (row_count,) = conn.execute(
        "SELECT COUNT(*) FROM fact_order_performance"
    ).fetchone()
    log.info("  ✓ fact_order_performance built  (%d rows)", row_count)


# ---------------------------------------------------------------------------
# Step 3 · Validate
# ---------------------------------------------------------------------------

def validate(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Run data-quality assertions against ``fact_order_performance``.

    Checks performed
    ----------------
    - Table must contain at least one row.
    - ``time_to_ship_hours`` must be non-negative for all rows where it is not NULL.
    - ``sla_status`` must only contain the values 'On-Time' or 'Breached' (or NULL).

    Parameters
    ----------
    conn : sqlite3.Connection
        Open connection to operations.db.

    Returns
    -------
    pd.DataFrame
        The full fact table, ready for export.

    Raises
    ------
    ValueError
        If one or more quality checks fail; the message lists every failure.
    """
    log.info("Loading fact table for validation …")
    df = pd.read_sql("SELECT * FROM fact_order_performance", conn)

    failures: list[str] = []

    # Check 1: result must not be empty
    if df.empty:
        failures.append("fact_order_performance contains zero rows.")

    # Check 2: no negative shipping times
    ship_col = df["time_to_ship_hours"].dropna()
    neg_ship = (ship_col < 0).sum()
    if neg_ship > 0:
        failures.append(
            f"{neg_ship} row(s) have negative time_to_ship_hours."
        )

    # Check 3: sla_status must only be 'On-Time', 'Breached', or NULL
    valid_sla = {"On-Time", "Breached"}
    bad_sla = df["sla_status"].dropna()
    bad_sla_values = set(bad_sla[~bad_sla.isin(valid_sla)].unique())
    if bad_sla_values:
        failures.append(
            f"Unexpected sla_status values found: {bad_sla_values}"
        )

    if failures:
        raise ValueError(
            "Validation failed with the following errors:\n"
            + "\n".join(f"  • {f}" for f in failures)
        )

    log.info("  ✓ All validation checks passed  (%d rows)", len(df))
    return df


# ---------------------------------------------------------------------------
# Step 4a · Export CSV
# ---------------------------------------------------------------------------

def export_csv(df: pd.DataFrame) -> None:
    """
    Write the gold fact table to a CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        Validated fact_order_performance DataFrame.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(GOLD_CSV_PATH, index=False)
    log.info("  ✓ Gold CSV written → %s  (%d rows)", GOLD_CSV_PATH, len(df))


# ---------------------------------------------------------------------------
# Step 4b · Export JSON (dashboard_data.json)
# ---------------------------------------------------------------------------

def _round1(value: float | None) -> float | None:
    """Round a float to 1 decimal place; return None if value is None/NaN."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return round(float(value), 1)


def build_dashboard_json(df: pd.DataFrame) -> dict[str, Any]:
    """
    Aggregate ``fact_order_performance`` into the structured payload expected
    by the React frontend.

    Parameters
    ----------
    df : pd.DataFrame
        Validated fact_order_performance DataFrame.

    Returns
    -------
    dict
        Full dashboard_data structure ready to be serialised to JSON.
    """
    total_orders: int = len(df)

    # ── meta ────────────────────────────────────────────────────────────────
    # Date range derived from order_date column
    order_dates = pd.to_datetime(df["order_date"], errors="coerce")
    date_from = order_dates.min().strftime("%Y-%m-%d")
    date_to   = order_dates.max().strftime("%Y-%m-%d")

    meta: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_orders": total_orders,
        "date_range": {"from": date_from, "to": date_to},
    }

    # ── kpis ────────────────────────────────────────────────────────────────
    # on_time_rate: share of orders with a known SLA verdict that are On-Time
    sla_known = df["sla_status"].dropna()
    on_time_rate = (
        round((sla_known == "On-Time").sum() / len(sla_known) * 100, 1)
        if len(sla_known) > 0
        else 0.0
    )

    # backorder_rate: share of all orders that were backordered
    backorder_rate = round(
        df["was_backordered"].sum() / total_orders * 100, 1
    ) if total_orders > 0 else 0.0

    # avg_cycle_time: mean time_to_deliver_hours across delivered orders
    avg_cycle = _round1(df["time_to_deliver_hours"].mean())

    kpis: dict[str, Any] = {
        "total_orders":      total_orders,
        "on_time_rate":      on_time_rate,
        "backorder_rate":    backorder_rate,
        "avg_cycle_time_hrs": avg_cycle,
    }

    # ── charts · team_ship_time ─────────────────────────────────────────────
    team_ship = (
        df.groupby("fulfillment_team")["time_to_ship_hours"]
        .mean()
        .reset_index()
        .rename(columns={"fulfillment_team": "team", "time_to_ship_hours": "avg_hours"})
        .sort_values("avg_hours", ascending=False)
    )
    team_ship_list = [
        {"team": row["team"], "avg_hours": _round1(row["avg_hours"])}
        for _, row in team_ship.iterrows()
    ]

    # ── charts · daily_volume ───────────────────────────────────────────────
    df["_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    daily_vol = (
        df.groupby("_date")
        .size()
        .reset_index(name="orders")
        .sort_values("_date")
    )
    daily_volume_list = [
        {"date": str(row["_date"]), "orders": int(row["orders"])}
        for _, row in daily_vol.iterrows()
    ]

    # ── charts · sla_by_priority ────────────────────────────────────────────
    sla_by_priority: list[dict[str, Any]] = []
    for priority in PRIORITIES:
        subset = df[df["priority"] == priority]
        on_time  = int((subset["sla_status"] == "On-Time").sum())
        breached = int((subset["sla_status"] == "Breached").sum())
        sla_by_priority.append(
            {"priority": priority, "on_time": on_time, "breached": breached}
        )

    # ── charts · orders_by_category ─────────────────────────────────────────
    cat_counts = (
        df.groupby("product_category")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    orders_by_category = [
        {"category": row["product_category"], "count": int(row["count"])}
        for _, row in cat_counts.iterrows()
    ]

    charts: dict[str, Any] = {
        "team_ship_time":       team_ship_list,
        "daily_volume":         daily_volume_list,
        "sla_by_priority":      sla_by_priority,
        "orders_by_category":   orders_by_category,
    }

    # ── table ────────────────────────────────────────────────────────────────
    # All non-cancelled orders sorted by order_date descending
    table_df = df.sort_values("order_date", ascending=False)
    table: list[dict[str, Any]] = [
        {
            "order_id":        row["order_id"],
            "buyer":           row["buyer_name"],
            "region":          row["region"],
            "team":            row["fulfillment_team"],
            "priority":        row["priority"],
            "ship_time_hrs":   _round1(row["time_to_ship_hours"]),
            "sla_status":      row["sla_status"],
            "bottleneck_stage": row["bottleneck_stage"],
            "was_backordered": int(row["was_backordered"]),
        }
        for _, row in table_df.iterrows()
    ]

    # ── filters ──────────────────────────────────────────────────────────────
    filters: dict[str, list[str]] = {
        "regions":    REGIONS,
        "priorities": PRIORITIES,
        "teams":      TEAMS,
        "categories": CATEGORIES,
    }

    return {
        "meta":    meta,
        "kpis":    kpis,
        "charts":  charts,
        "table":   table,
        "filters": filters,
    }


def export_json(df: pd.DataFrame) -> None:
    """
    Build and write ``dashboard_data.json`` to the outputs directory.

    Parameters
    ----------
    df : pd.DataFrame
        Validated fact_order_performance DataFrame.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Building dashboard JSON payload …")
    payload = build_dashboard_json(df)

    with JSON_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)

    log.info(
        "  ✓ dashboard_data.json written → %s  (%d table rows)",
        JSON_PATH,
        len(payload["table"]),
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Orchestrate the full ETL pipeline: ingest → transform → validate → export."""
    log.info("=" * 60)
    log.info("  Flow ETL Pipeline — starting")
    log.info("=" * 60)

    # Ensure the database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        # Step 1 — Ingest raw CSVs into SQLite
        log.info("── Step 1 · Ingest ──────────────────────────────────────")
        ingest(conn)

        # Step 2 — Run SQL transformation to build fact table
        log.info("── Step 2 · Transform ───────────────────────────────────")
        transform(conn)

        # Step 3 — Validate output quality
        log.info("── Step 3 · Validate ────────────────────────────────────")
        fact_df = validate(conn)

    # Step 4 — Export artefacts (outside the connection context)
    log.info("── Step 4 · Export ──────────────────────────────────────")
    export_csv(fact_df)
    export_json(fact_df)

    log.info("=" * 60)
    log.info("  Flow ETL Pipeline — complete ✓")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
