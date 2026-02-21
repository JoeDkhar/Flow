"""
etl_pipeline.py
Ingests raw CSVs into SQLite and runs SQL transformations.

Run:
    python scripts/etl_pipeline.py
"""

import sqlite3
from pathlib import Path
import pandas as pd

ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / "operations.db"
RAW_DIR    = ROOT / "data" / "raw"
SQL_PATH   = ROOT / "sql_queries" / "transformations.sql"
OUT_DIR    = ROOT / "outputs"
OUT_DIR.mkdir(exist_ok=True)


def ingest(conn: sqlite3.Connection) -> None:
    """Load raw CSVs into SQLite as staging tables."""
    orders_df = pd.read_csv(RAW_DIR / "orders.csv")
    logs_df   = pd.read_csv(RAW_DIR / "order_logs.csv")
    orders_df.to_sql("raw_orders", conn, if_exists="replace", index=False)
    logs_df.to_sql("raw_logs",   conn, if_exists="replace", index=False)
    print(f"  Ingested {len(orders_df):,} orders and {len(logs_df):,} log rows")


if __name__ == "__main__":
    print("Running ETL pipeline...")
    conn = sqlite3.connect(DB_PATH)
    ingest(conn)
    conn.close()
    print("Ingestion complete. SQL transformations coming next.")
