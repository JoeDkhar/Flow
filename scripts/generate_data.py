"""
generate_data.py
Generates synthetic wholesale order and status-log data for the Flow project.

The data models a wholesale distributor's order fulfillment pipeline with
realistic inter-stage delays, a deliberate Team C bottleneck, backorders,
and cancellations.

Run:
    python scripts/generate_data.py
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# ── Reproducibility ────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REGIONS     = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
TEAMS       = ["Team A", "Team B", "Team C", "Team D"]
PRIORITIES  = ["Standard", "Expedited", "Critical"]
CATEGORIES  = ["Electronics", "Apparel", "Industrial", "Food & Bev", "Home Goods"]

N_ORDERS          = 2_000
BACKORDER_RATE    = 0.15   # 15 % of orders hit a backorder state
CANCELLATION_RATE = 0.05   # 5 % of orders cancel mid-pipeline

# Realistic delay ranges (hours) per status transition.
# Team C gets a multiplier applied to Processing → Picking to simulate a bottleneck.
STAGE_DELAYS: dict[str, tuple[float, float]] = {
    "Received→Confirmed":  (1,   4),
    "Confirmed→Processing":(2,   12),
    "Processing→Picking":  (4,   24),   # Team C uses TEAM_C_MULTIPLIER
    "Picking→Packed":      (1,   6),
    "Packed→Shipped":      (2,   8),
    "Shipped→Delivered":   (24,  120),   # wide range; region-adjusted below
}

# Delivery time adjustments by region (multiplier on the Shipped→Delivered range)
REGION_DELIVERY_FACTOR: dict[str, float] = {
    "Northeast": 0.7,
    "Southeast": 0.9,
    "Midwest":   1.0,
    "Southwest": 1.2,
    "West":      1.4,
}

TEAM_C_BOTTLENECK_MULTIPLIER = 3.1   # increased — bottleneck was not visible in charts   # Team C Processing→Picking is ~3x slower


# ── Data generation ────────────────────────────────────────────────────────

def _random_delay(stage: str, team: str) -> timedelta:
    """Return a random delay for a given stage, accounting for team bottleneck."""
    lo, hi = STAGE_DELAYS[stage]
    if stage == "Processing→Picking" and team == "Team C":
        lo *= TEAM_C_BOTTLENECK_MULTIPLIER
        hi *= TEAM_C_BOTTLENECK_MULTIPLIER
    hours = random.uniform(lo, hi)
    return timedelta(hours=hours)


def generate_orders(n: int) -> pd.DataFrame:
    """Create n synthetic wholesale purchase orders."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "order_id":         f"ORD-{i:05d}",
            "buyer_name":       fake.company(),
            "region":           random.choice(REGIONS),
            "fulfillment_team": random.choice(TEAMS),
            "priority":         random.choices(
                                    PRIORITIES, weights=[60, 30, 10], k=1
                                )[0],
            "product_category": random.choice(CATEGORIES),
            "order_date":       fake.date_time_between(
                                    start_date="-6M", end_date="now"
                                ).strftime("%Y-%m-%d %H:%M:%S"),
            "order_value_usd":  round(random.uniform(500, 50_000), 2),
            "total_units":      random.randint(10, 500),
        })
    return pd.DataFrame(rows)


def generate_logs(orders: pd.DataFrame) -> pd.DataFrame:
    """
    For each order, walk through the fulfillment pipeline and emit a
    status-change log row at each transition.

    Orders may branch into Backordered (then resume) or Cancelled states.
    """
    pipeline = [
        "Received",
        "Confirmed",
        "Processing",
        "Picking",
        "Packed",
        "Shipped",
        "Delivered",
    ]
    # Maps consecutive statuses to their delay key
    stage_keys = {
        ("Received",    "Confirmed"):  "Received→Confirmed",
        ("Confirmed",   "Processing"): "Confirmed→Processing",
        ("Processing",  "Picking"):    "Processing→Picking",
        ("Picking",     "Packed"):     "Picking→Packed",
        ("Packed",      "Shipped"):    "Packed→Shipped",
        ("Shipped",     "Delivered"):  "Shipped→Delivered",
    }

    log_rows = []
    log_id = 1

    for _, order in orders.iterrows():
        order_dt   = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
        team       = order["fulfillment_team"]
        region     = order["region"]
        cancel_at  = random.choice(["Processing", "Picking", "Packed"])                              if random.random() < CANCELLATION_RATE else None
        backorder_at = random.choice(["Processing", "Picking"])                                if random.random() < BACKORDER_RATE else None

        current_dt = order_dt

        # Emit the first "Received" log
        log_rows.append({
            "log_id":   log_id,
            "order_id": order["order_id"],
            "status":   "Received",
            "timestamp": current_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "notes":    "",
        })
        log_id += 1

        for i in range(len(pipeline) - 1):
            src, dst = pipeline[i], pipeline[i + 1]
            stage_key = (src, dst)

            # Cancellation branch
            if cancel_at == dst:
                current_dt += _random_delay(stage_keys[stage_key], team)
                log_rows.append({
                    "log_id":   log_id,
                    "order_id": order["order_id"],
                    "status":   "Cancelled",
                    "timestamp": current_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "notes":    "Customer request",
                })
                log_id += 1
                break

            # Backorder branch (pause then resume)
            if backorder_at == dst:
                current_dt += _random_delay(stage_keys[stage_key], team)
                log_rows.append({
                    "log_id":   log_id,
                    "order_id": order["order_id"],
                    "status":   "Backordered",
                    "timestamp": current_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "notes":    "Inventory shortage",
                })
                log_id += 1
                backorder_at = None   # only backorder once
                current_dt += timedelta(hours=random.uniform(24, 72))

            # Adjust delivery delay by region
            delay_key = stage_keys[stage_key]
            if delay_key == "Shipped→Delivered":
                lo, hi = STAGE_DELAYS[delay_key]
                factor = REGION_DELIVERY_FACTOR.get(region, 1.0)
                current_dt += timedelta(hours=random.uniform(lo * factor, hi * factor))
            else:
                current_dt += _random_delay(delay_key, team)

            log_rows.append({
                "log_id":   log_id,
                "order_id": order["order_id"],
                "status":   dst,
                "timestamp": current_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "notes":    "",
            })
            log_id += 1

    return pd.DataFrame(log_rows)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating synthetic wholesale order data (seed=42)...")

    print("  Building orders...")
    orders_df = generate_orders(N_ORDERS)
    orders_path = OUTPUT_DIR / "orders.csv"
    orders_df.to_csv(orders_path, index=False)
    print(f"  Wrote {len(orders_df):,} orders → {orders_path}")

    print("  Building order logs...")
    logs_df = generate_logs(orders_df)
    logs_path = OUTPUT_DIR / "order_logs.csv"
    logs_df.to_csv(logs_path, index=False)
    print(f"  Wrote {len(logs_df):,} log rows → {logs_path}")

    print("Done. Next: python scripts/etl_pipeline.py")
