-- transformations.sql
-- Full pipeline: raw logs → stage intervals → bottleneck → SLA → gold fact table
--
-- Execution order (handled by etl_pipeline.py):
--   1. ranked_logs       — deterministic ordering within each order
--   2. stage_intervals  — LEAD() window to derive time-in-stage per stage
--   3. order_timeline    — aggregate key timestamps per order
--   4. bottleneck_stages — identify the single stage with longest dwell time
--   5. fact_order_performance — final gold table joined with orders metadata


-- ── CTE 1: Rank log entries per order by timestamp ──────────────────────
WITH ranked_logs AS (
    SELECT
        order_id,
        stage,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp) AS rn
    FROM raw_logs
),

-- ── CTE 2: Pair each stage row with the next using LEAD() ───────────────
-- This gives us (stage, next_stage, entered_at, exited_at, hours_in_stage)
-- for every stage transition an order went through.
stage_intervals AS (
    SELECT
        order_id,
        stage                                                          AS from_stage,
        LEAD(stage)    OVER (PARTITION BY order_id ORDER BY timestamp) AS to_stage,
        timestamp                                                        AS entered_at,
        LEAD(timestamp) OVER (PARTITION BY order_id ORDER BY timestamp)  AS exited_at,
        ROUND(
            (JULIANDAY(
                LEAD(timestamp) OVER (PARTITION BY order_id ORDER BY timestamp)
            ) - JULIANDAY(timestamp)) * 24,
            2
        )                                                                AS hours_in_stage
    FROM raw_logs
),

-- ── CTE 3: Derive key milestone timestamps per order ─────────────────────
-- We pull the timestamp at each named stage milestone via MIN() over a filter.
-- NULLIF handles orders that never reached Shipped or Delivered (cancelled early).
order_timeline AS (
    SELECT
        order_id,
        MIN(CASE WHEN from_stage = 'Confirmed'   THEN entered_at END) AS confirmed_at,
        MIN(CASE WHEN from_stage = 'Shipped'     THEN entered_at END) AS shipped_at,
        MIN(CASE WHEN from_stage = 'Delivered'   THEN entered_at END) AS delivered_at,
        MAX(CASE WHEN from_stage = 'Backordered' THEN 1 ELSE 0 END)   AS was_backordered,
        MAX(CASE WHEN from_stage = 'Cancelled'   THEN 1 ELSE 0 END)   AS was_cancelled,
        ROUND(SUM(CASE
            WHEN from_stage NOT IN ('Backordered','Cancelled','Delivered')
            THEN hours_in_stage ELSE 0
        END), 2)                                                         AS total_active_hours
    FROM stage_intervals
    GROUP BY order_id
),

-- ── CTE 4: Identify the bottleneck stage (longest dwell time per order) ──
-- Uses a sub-select with ROW_NUMBER to pick the single worst stage.
-- Excludes terminal/anomaly states that aren't part of the core pipeline.
ranked_stages AS (
    SELECT
        order_id,
        from_stage,
        hours_in_stage,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY hours_in_stage DESC
        ) AS stage_rank
    FROM stage_intervals
    WHERE
        to_stage IS NOT NULL
        AND from_stage NOT IN ('Backordered', 'Cancelled', 'Delivered')
),

bottleneck_stages AS (
    SELECT order_id, from_stage AS bottleneck_stage
    FROM ranked_stages
    WHERE stage_rank = 1
),

-- ── CTE 5: Compute derived time metrics and SLA stage ───────────────────
order_metrics AS (
    SELECT
        o.order_id,
        t.confirmed_at,
        t.shipped_at,
        t.delivered_at,
        t.was_backordered,
        t.was_cancelled,
        -- Time from order receipt to confirmation (hours)
        ROUND(
            (JULIANDAY(t.confirmed_at) - JULIANDAY(o.order_date)) * 24, 2
        )                                                              AS time_to_confirm_hours,
        -- Time from order receipt to shipment (the primary SLA metric)
        ROUND(
            (JULIANDAY(t.shipped_at) - JULIANDAY(o.order_date)) * 24, 2
        )                                                              AS time_to_ship_hours,
        -- Time from order receipt to final delivery (NULL if not yet delivered)
        ROUND(
            (JULIANDAY(t.delivered_at) - JULIANDAY(o.order_date)) * 24, 2
        )                                                              AS time_to_deliver_hours,
        -- SLA breach logic by priority tier
        CASE o.priority
            WHEN 'Critical'  THEN
                CASE WHEN JULIANDAY(t.shipped_at) - JULIANDAY(o.order_date) > 1.0 THEN 'Breached' ELSE 'On-Time' END
            WHEN 'Expedited' THEN
                CASE WHEN JULIANDAY(t.shipped_at) - JULIANDAY(o.order_date) > 2.0 THEN 'Breached' ELSE 'On-Time' END
            ELSE   -- Standard
                CASE WHEN JULIANDAY(t.shipped_at) - JULIANDAY(o.order_date) > 3.0 THEN 'Breached' ELSE 'On-Time' END
        END                                                            AS sla_stage
    FROM raw_orders o
    JOIN order_timeline t ON o.order_id = t.order_id
)


-- ── Final SELECT: gold-layer fact table ───────────────────────────────────
SELECT
    m.order_id,
    o.buyer_name,
    o.region,
    o.fulfillment_team,
    o.priority,
    o.product_category,
    o.order_value_usd,
    o.total_units,
    o.order_date,
    m.time_to_confirm_hours,
    m.time_to_ship_hours,
    m.time_to_deliver_hours,
    m.was_backordered,
    m.was_cancelled,
    m.sla_stage,
    CASE WHEN m.sla_stage = 'Breached' THEN 1 ELSE 0 END AS sla_breached,
    b.bottleneck_stage
FROM order_metrics m
JOIN raw_orders o ON m.order_id = o.order_id
LEFT JOIN bottleneck_stages b ON m.order_id = b.order_id
WHERE m.was_cancelled = 0;   -- Exclude cancelled orders from performance metrics
