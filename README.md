# Flow — B2B Wholesale Order Fulfillment Analytics

An end-to-end data engineering and analytics project tracking order fulfillment
performance for a wholesale distributor serving retail buyers across North America.

## Project Structure

```
Flow/
├── data/raw/
│   ├── orders.csv          # 2,000 synthetic wholesale orders
│   └── order_logs.csv      # ~8,000 status-change events
├── scripts/
│   ├── generate_data.py    # Synthetic data generation (seed=42)
│   └── etl_pipeline.py     # ETL + SQL transformations → SQLite
├── sql_queries/
│   └── transformations.sql # CTEs, LEAD() window functions
├── outputs/
│   └── gold_order_performance.csv
├── operations.db
├── dashboard.py            # Streamlit dashboard
└── viz_requirements.txt
```

## Quick Start

```bash
pip install -r viz_requirements.txt
python scripts/generate_data.py
python scripts/etl_pipeline.py
streamlit run dashboard.py
```

## Technical Details

### Data Generation
- 2,000 wholesale orders across 5 regions and 4 fulfillment teams
- ~8,000 status-change log events with realistic inter-stage delays
- Intentional bottleneck in Team C (Processing → Picking stage)
- ~15% backorder rate, ~5% cancellation rate

### SQL Transformations
- `LEAD()` window function to calculate time-in-status per stage per order
- CTEs for bottleneck detection and SLA tier evaluation
- Gold-layer `fact_order_performance` table for dashboard consumption

### KPIs Tracked
- On-Time Delivery Rate by SLA tier (Critical <24h, Expedited <48h, Standard <72h)
- Average order cycle time (Received → Delivered)
- Backorder rate and cancellation rate
- Stage-level bottleneck analysis by fulfillment team

## Author

**Josaiah Murfeal Dkhar**  
MCA, CHRIST (Deemed to be University), Bengaluru  
[github.com/JoeDkhar](https://github.com/JoeDkhar)

*March 2026 — Portfolio Project: B2B Operations Analytics System*
