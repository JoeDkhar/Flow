"""
etl_pipeline.py
Ingests raw CSVs into SQLite and produces the gold-layer fact table
via SQL transformations.

Run:
    python scripts/etl_pipeline.py
"""

import sqlite3
import logging
from pathlib import Path

import pandas as pd

# ── Logging setup ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent.parent
DB_PATH  = ROOT / "operations.db"
RAW_DIR  = ROOT / "data" / "raw"
SQL_PATH = ROOT / "sql_queries" / "transformations.sql"
OUT_DIR  = ROOT / "outputs"
OUT_DIR.mkdir(exist_ok=True)


# ── Pipeline steps ─────────────────────────────────────────────────────────

def ingest(conn: sqlite3.Connection) -> None:
    """Load raw CSVs into SQLite as staging tables (raw_orders, raw_logs)."""
    orders_path = RAW_DIR / "orders.csv"
    logs_path   = RAW_DIR / "order_logs.csv"

    for path in (orders_path, logs_path):
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found. Run `python scripts/generate_data.py` first."
            )

    orders_df = pd.read_csv(orders_path, parse_dates=["order_date"])
    logs_df   = pd.read_csv(logs_path,   parse_dates=["timestamp"])

    orders_df.to_sql("raw_orders", conn, if_exists="replace", index=False)
    logs_df.to_sql(  "raw_logs",   conn, if_exists="replace", index=False)

    log.info("Ingested %s orders and %s log rows", f"{len(orders_df):,}", f"{len(logs_df):,}")


def transform(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Execute the SQL transformation script and materialise the gold fact table.

    Returns the resulting DataFrame for downstream validation.
    """
    sql = SQL_PATH.read_text()

    # The SQL file is a single SELECT statement (a chain of CTEs).
    # We wrap it in a CREATE TABLE AS to persist results in SQLite.
    create_sql = f"DROP TABLE IF EXISTS fact_order_performance;\n"
    create_sql += f"CREATE TABLE fact_order_performance AS\n{sql};"

    conn.executescript(create_sql)
    conn.commit()

    result_df = pd.read_sql("SELECT * FROM fact_order_performance", conn)
    log.info("Transformed %s orders into fact table", f"{len(result_df):,}")
    return result_df


def export(df: pd.DataFrame) -> None:
    """Write the gold fact table to a CSV for Power BI or ad-hoc analysis."""
    out_path = OUT_DIR / "gold_order_performance.csv"
    df.to_csv(out_path, index=False)
    log.info("Exported gold layer → %s", out_path)


def validate(df: pd.DataFrame) -> None:
    """
    Basic sanity checks on the transformed output.
    Raises ValueError if expectations are not met.
    """
    checks = {
        "No rows produced":        len(df) > 0,
        "Missing order_id":        df["order_id"].notna().all(),
        "time_to_ship negative":   (df["time_to_ship_hours"].dropna() >= 0).all(),
        "Unknown sla_status":      df["sla_status"].isin(["On-Time", "Breached"]).all(),
    }
    failures = [msg for msg, passed in checks.items() if not passed]
    if failures:
        raise ValueError("Validation failed:\n  " + "\n  ".join(failures))
    log.info("Validation passed (%d checks)", len(checks))


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting Flow ETL pipeline")

    with sqlite3.connect(DB_PATH) as conn:
        ingest(conn)
        fact_df = transform(conn)
        validate(fact_df)
        export(fact_df)

    log.info("Pipeline complete. Launch dashboard: streamlit run dashboard.py")
