# Flow â€” B2B Wholesale Order Fulfillment Analytics

An end-to-end data engineering and analytics project tracking order fulfillment
performance for a wholesale distributor serving retail buyers across North America.

## Project Structure

```
Flow/
â”œâ”€â”€ data/raw/
â”‚   â”œâ”€â”€ orders.csv          # 2,000 synthetic wholesale orders
â”‚   â””â”€â”€ order_logs.csv      # ~8,000 status-change events
â”œâ”€â”€ scripts/
â”‚   â”œâ”€â”€ generate_data.py    # Synthetic data generation (seed=42)
â”‚   â””â”€â”€ etl_pipeline.py     # ETL + SQL transformations â†’ SQLite
â”œâ”€â”€ sql_queries/
â”‚   â””â”€â”€ transformations.sql # CTEs, LEAD() window functions
â”œâ”€â”€ outputs/
â”‚   â””â”€â”€ gold_order_performance.csv
â”œâ”€â”€ operations.db
â”œâ”€â”€ dashboard.py            # Streamlit dashboard
â””â”€â”€ viz_requirements.txt
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
- Intentional bottleneck in Team C (Processing â†’ Picking stage)
- ~15% backorder rate, ~5% cancellation rate

### SQL Transformations
- `LEAD()` window function to calculate time-in-status per stage per order
- CTEs for bottleneck detection and SLA tier evaluation
- Gold-layer `fact_order_performance` table for dashboard consumption

### KPIs Tracked
- On-Time Delivery Rate by SLA tier (Critical <24h, Expedited <48h, Standard <72h)
- Average order cycle time (Received â†’ Delivered)
- Backorder rate and cancellation rate
- Stage-level bottleneck analysis by fulfillment team

## Author

**Josaiah Murfeal Dkhar**  
MCA, CHRIST (Deemed to be University), Bengaluru  
[github.com/JoeDkhar](https://github.com/JoeDkhar)

*March 2026 â€” Portfolio Project: B2B Operations Analytics System*

