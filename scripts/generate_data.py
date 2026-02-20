        """
        generate_data.py
        Generates synthetic wholesale order data for the Flow analytics project.

        Run:
            python scripts/generate_data.py
        """

        import random
        from pathlib import Path
        import pandas as pd
        from faker import Faker

        SEED = 42
        random.seed(SEED)
        fake = Faker()
        Faker.seed(SEED)

        OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
        TEAMS = ["Team A", "Team B", "Team C", "Team D"]
        PRIORITIES = ["Standard", "Expedited", "Critical"]
        CATEGORIES = ["Electronics", "Apparel", "Industrial", "Food & Bev", "Home Goods"]
        N_ORDERS = 2_000


        def generate_orders(n: int) -> pd.DataFrame:
            """Create n synthetic wholesale purchase orders."""
            rows = []
            for i in range(1, n + 1):
                rows.append({
                    "order_id":       f"ORD-{i:05d}",
                    "buyer_name":     fake.company(),
                    "region":         random.choice(REGIONS),
                    "fulfillment_team": random.choice(TEAMS),
                    "priority":       random.choices(
                        PRIORITIES, weights=[60, 30, 10], k=1
                    )[0],
                    "product_category": random.choice(CATEGORIES),
                    "order_date":     fake.date_time_between(
                        start_date="-6M", end_date="now"
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "order_value_usd": round(random.uniform(500, 50_000), 2),
                    "total_units":    random.randint(10, 500),
                })
            return pd.DataFrame(rows)


        if __name__ == "__main__":
            print("Generating orders...")
            orders_df = generate_orders(N_ORDERS)
            orders_path = OUTPUT_DIR / "orders.csv"
            orders_df.to_csv(orders_path, index=False)
            print(f"  Wrote {len(orders_df):,} rows → {orders_path}")
            # WIP: log generation below
print("Orders written. Adding logs next...")
