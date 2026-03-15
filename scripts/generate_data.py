"""
generate_data.py
================
Synthetic data generator for the Flow wholesale order fulfillment pipeline.

Generates two CSV files:
  - data/raw/orders.csv      : 2,000 orders with metadata
  - data/raw/order_logs.csv  : ~8,000 stage-level event rows per order lifecycle

Run:
    python scripts/generate_data.py

Dependencies:
    pandas, faker, numpy  (plus stdlib: random, datetime, pathlib)
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
SEED: int = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path("data/raw")
ORDERS_PATH = OUTPUT_DIR / "orders.csv"
LOGS_PATH = OUTPUT_DIR / "order_logs.csv"

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------
NUM_ORDERS: int = 2_000

REGIONS: List[str] = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]

FULFILLMENT_TEAMS: List[str] = ["Team A", "Team B", "Team C", "Team D"]

# Priority distribution: Standard 60%, Expedited 30%, Critical 10%
PRIORITY_CHOICES: List[str] = ["Standard", "Expedited", "Critical"]
PRIORITY_WEIGHTS: List[float] = [0.60, 0.30, 0.10]

PRODUCT_CATEGORIES: List[str] = [
    "Electronics",
    "Apparel",
    "Industrial",
    "Food & Bev",
    "Home Goods",
]

# Stage pipeline in order
STAGES: List[str] = [
    "Received",
    "Confirmed",
    "Processing",
    "Picking",
    "Packed",
    "Shipped",
    "Delivered",
]

# Delay hours (min, max) between consecutive stage transitions
STAGE_DELAYS_HOURS: Dict[str, tuple[float, float]] = {
    "Received->Confirmed":   (1, 4),
    "Confirmed->Processing": (2, 12),
    "Processing->Picking":   (4, 24),   # Team C applies 3.1x multiplier here
    "Picking->Packed":       (1, 6),
    "Packed->Shipped":       (2, 8),
    "Shipped->Delivered":    (24, 120), # Region delivery multiplier applied here
}

# Regional multiplier on the Shipped->Delivered delay
REGION_DELIVERY_MULTIPLIERS: Dict[str, float] = {
    "Northeast": 0.7,
    "Southeast": 0.9,
    "Midwest":   1.0,
    "Southwest": 1.2,
    "West":      1.4,
}

# Team C bottleneck multiplier for Processing->Picking transition
TEAM_C_BOTTLENECK_MULTIPLIER: float = 3.1

# Special-event probabilities
BACKORDER_PROB: float = 0.15   # 15% of orders hit Backordered mid-pipeline
CANCEL_PROB: float = 0.05      # 5% of orders are Cancelled at a mid-stage

# Backorder delay range (hours)
BACKORDER_DELAY_HOURS: tuple[float, float] = (24, 72)

# Order value and unit ranges
ORDER_VALUE_RANGE: tuple[float, float] = (500.0, 50_000.0)
TOTAL_UNITS_RANGE: tuple[int, int] = (10, 500)

# Look-back window for order_date generation (days)
ORDER_DATE_LOOKBACK_DAYS: int = 180


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def random_hours(min_h: float, max_h: float) -> float:
    """Return a uniformly distributed random number of hours in [min_h, max_h]."""
    return random.uniform(min_h, max_h)


def advance_time(current: datetime, hours: float) -> datetime:
    """Return a new datetime advanced by the given fractional hours."""
    return current + timedelta(hours=hours)


def make_order_id(index: int) -> str:
    """Format a zero-padded order ID string, e.g. ORD-00042."""
    return f"ORD-{index:05d}"


# ---------------------------------------------------------------------------
# Order generation
# ---------------------------------------------------------------------------

def generate_orders(n: int = NUM_ORDERS) -> pd.DataFrame:
    """
    Build the orders DataFrame with n synthetic wholesale orders.

    Parameters
    ----------
    n : int
        Number of orders to generate.

    Returns
    -------
    pd.DataFrame
        Orders table with columns matching the spec.
    """
    print(f"[1/3] Generating {n:,} orders ...")

    now = datetime.now()
    lookback_start = now - timedelta(days=ORDER_DATE_LOOKBACK_DAYS)

    rows: List[Dict[str, Any]] = []

    for i in range(1, n + 1):
        # Unique order identifier
        order_id = make_order_id(i)

        # Company name via Faker
        buyer_name = fake.company()

        # Categorical fields
        region = random.choice(REGIONS)
        fulfillment_team = random.choice(FULFILLMENT_TEAMS)
        priority = random.choices(PRIORITY_CHOICES, weights=PRIORITY_WEIGHTS, k=1)[0]
        product_category = random.choice(PRODUCT_CATEGORIES)

        # Random datetime within last 6 months (stored as naive UTC for simplicity)
        seconds_back = random.uniform(0, ORDER_DATE_LOOKBACK_DAYS * 86_400)
        order_date = lookback_start + timedelta(seconds=seconds_back)

        # Numeric fields
        order_value_usd = round(random.uniform(*ORDER_VALUE_RANGE), 2)
        total_units = random.randint(*TOTAL_UNITS_RANGE)

        rows.append(
            {
                "order_id":          order_id,
                "buyer_name":        buyer_name,
                "region":            region,
                "fulfillment_team":  fulfillment_team,
                "priority":          priority,
                "product_category":  product_category,
                "order_date":        order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "order_value_usd":   order_value_usd,
                "total_units":       total_units,
            }
        )

    df = pd.DataFrame(rows)
    print(f"    ✓ Generated {len(df):,} order rows.")
    return df


# ---------------------------------------------------------------------------
# Log generation
# ---------------------------------------------------------------------------

def generate_logs(orders: pd.DataFrame) -> pd.DataFrame:
    """
    For each order, walk through the fulfillment stage pipeline and emit
    one log row per stage transition (and extra rows for Backordered /
    Cancelled events when triggered).

    Parameters
    ----------
    orders : pd.DataFrame
        Output from generate_orders().

    Returns
    -------
    pd.DataFrame
        order_logs table with one row per stage event.
    """
    print("[2/3] Generating order logs ...")

    log_rows: List[Dict[str, Any]] = []

    # Pre-determine which orders are backordered or cancelled (for reproducibility)
    backorder_flags = np.random.random(len(orders)) < BACKORDER_PROB
    cancel_flags    = np.random.random(len(orders)) < CANCEL_PROB

    for idx, order in orders.iterrows():
        order_id        = order["order_id"]
        region          = order["region"]
        fulfillment_team = order["fulfillment_team"]
        order_date_str  = order["order_date"]

        # Parse the order_date as the timestamp for the first stage
        current_time = datetime.strptime(order_date_str, "%Y-%m-%d %H:%M:%S")

        is_backordered = backorder_flags[idx]
        is_cancelled   = cancel_flags[idx]

        # Pick a random mid-stage (index 1-5, i.e., not first or last) for
        # these special events to occur
        backorder_stage_idx = random.randint(1, len(STAGES) - 3)  # mid-pipeline
        cancel_stage_idx    = random.randint(1, len(STAGES) - 2)  # mid-pipeline

        # Track whether special events have already fired
        backorder_fired = False
        order_cancelled = False

        for stage_idx, stage in enumerate(STAGES):
            # Emit the current stage as entered
            log_rows.append(
                {
                    "order_id":  order_id,
                    "stage":     stage,
                    "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "event":     "stage_entered",
                    "notes":     "",
                }
            )

            # ----------------------------------------------------------------
            # Check: Backordered event (fires once, at the designated mid-stage)
            # ----------------------------------------------------------------
            if (
                is_backordered
                and not backorder_fired
                and stage_idx == backorder_stage_idx
            ):
                backorder_delay = random_hours(*BACKORDER_DELAY_HOURS)
                current_time = advance_time(current_time, backorder_delay)
                log_rows.append(
                    {
                        "order_id":  order_id,
                        "stage":     "Backordered",
                        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "event":     "backordered",
                        "notes":     f"Delay {backorder_delay:.1f} hrs before resuming",
                    }
                )
                backorder_fired = True

            # ----------------------------------------------------------------
            # Check: Cancelled event (fires once, terminates the pipeline)
            # ----------------------------------------------------------------
            if (
                is_cancelled
                and not order_cancelled
                and stage_idx == cancel_stage_idx
            ):
                log_rows.append(
                    {
                        "order_id":  order_id,
                        "stage":     "Cancelled",
                        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "event":     "cancelled",
                        "notes":     f"Order cancelled at {stage}",
                    }
                )
                order_cancelled = True
                break  # No further stages after cancellation

            # ----------------------------------------------------------------
            # Advance time to the next stage (if not at Delivered yet)
            # ----------------------------------------------------------------
            if stage_idx < len(STAGES) - 1:
                transition_key = f"{stage}->{STAGES[stage_idx + 1]}"
                min_h, max_h = STAGE_DELAYS_HOURS[transition_key]
                delay_hours = random_hours(min_h, max_h)

                # Apply Team C bottleneck on Processing->Picking
                if (
                    transition_key == "Processing->Picking"
                    and fulfillment_team == "Team C"
                ):
                    delay_hours *= TEAM_C_BOTTLENECK_MULTIPLIER

                # Apply regional delivery multiplier on Shipped->Delivered
                if transition_key == "Shipped->Delivered":
                    multiplier = REGION_DELIVERY_MULTIPLIERS[region]
                    delay_hours *= multiplier

                current_time = advance_time(current_time, delay_hours)

    df = pd.DataFrame(log_rows)
    print(f"    ✓ Generated {len(df):,} log rows.")
    return df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Orchestrate data generation and write CSVs to disk."""
    print("=" * 60)
    print("  Flow — Synthetic Data Generator")
    print("=" * 60)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate orders
    orders_df = generate_orders()

    # Generate logs (depends on orders)
    logs_df = generate_logs(orders_df)

    # ------------------------------------------------------------------
    # Write to disk
    # ------------------------------------------------------------------
    print("[3/3] Writing CSV files ...")

    orders_df.to_csv(ORDERS_PATH, index=False)
    print(f"    ✓ Wrote {len(orders_df):,} rows → {ORDERS_PATH}")

    logs_df.to_csv(LOGS_PATH, index=False)
    print(f"    ✓ Wrote {len(logs_df):,} rows → {LOGS_PATH}")

    print("=" * 60)
    print("  Done! Data files are ready.")
    print("=" * 60)


if __name__ == "__main__":
    main()
