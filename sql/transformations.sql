-- =============================================================================
-- transformations.sql
-- Flow Wholesale Order Fulfillment Pipeline — ETL Transformation Layer
--
-- Executed as:
--   CREATE TABLE fact_order_performance AS <this SQL>
--
-- Input tables:
--   raw_orders  : order-level metadata (one row per order)
--   raw_logs    : stage-level event log  (many rows per order)
--
-- Output table:
--   fact_order_performance : one enriched row per non-cancelled order,
--                            ready for analytics and BI consumption.
-- =============================================================================

WITH

-- =============================================================================
-- CTE 1 · stage_intervals
-- -----------------------------------------------------------------------------
-- Pairs each log row with the immediately following log row for the same order
-- using LEAD() so we can measure the time the order spent in each stage.
-- hours_in_stage is NULL for the terminal row of each order (no successor).
-- Columns: order_id, from_stage, to_stage, entered_at, exited_at,
--          hours_in_stage
-- =============================================================================
stage_intervals AS (
    SELECT
        order_id,

        -- The stage that was entered on this log row
        stage                                              AS from_stage,

        -- The stage that immediately follows within the same order
        LEAD(stage)      OVER (
            PARTITION BY order_id
            ORDER BY     timestamp
        )                                                   AS to_stage,

        -- Timestamp this stage was entered
        timestamp                                           AS entered_at,

        -- Timestamp the next stage was entered (= when this one was exited)
        LEAD(timestamp)   OVER (
            PARTITION BY order_id
            ORDER BY     timestamp
        )                                                   AS exited_at,

        -- Duration in hours, rounded to 2 d.p.; NULL for the last row per order
        ROUND(
            (
                JULIANDAY(
                    LEAD(timestamp) OVER (
                        PARTITION BY order_id
                        ORDER BY     timestamp
                    )
                ) - JULIANDAY(timestamp)
            ) * 24.0,
            2
        )                                                   AS hours_in_stage

    FROM raw_logs
),

-- =============================================================================
-- CTE 2 · order_timeline
-- -----------------------------------------------------------------------------
-- Collapses the per-row log down to key milestone timestamps for every order,
-- plus boolean flags indicating whether special events occurred.
--   confirmed_at    : earliest timestamp the order entered "Confirmed"
--   shipped_at      : earliest timestamp the order entered "Shipped"
--   delivered_at    : earliest timestamp the order entered "Delivered"
--   was_backordered : 1 if any "Backordered" row exists for this order
--   was_cancelled   : 1 if any "Cancelled"   row exists for this order
-- =============================================================================
order_timeline AS (
    SELECT
        order_id,

        -- First time the order reached each key milestone
        MIN(CASE WHEN stage = 'Confirmed'  THEN timestamp END) AS confirmed_at,
        MIN(CASE WHEN stage = 'Shipped'    THEN timestamp END) AS shipped_at,
        MIN(CASE WHEN stage = 'Delivered'  THEN timestamp END) AS delivered_at,

        -- Flag: was the order ever placed in a Backordered state?
        MAX(CASE WHEN stage = 'Backordered' THEN 1 ELSE 0 END) AS was_backordered,

        -- Flag: was the order ever cancelled?
        MAX(CASE WHEN stage = 'Cancelled'   THEN 1 ELSE 0 END) AS was_cancelled

    FROM raw_logs
    GROUP BY order_id
),

-- =============================================================================
-- CTE 3 · ranked_stages
-- -----------------------------------------------------------------------------
-- Assigns a ROW_NUMBER() rank to every stage interval per order, ordering by
-- hours_in_stage DESC so rank 1 = the single stage that consumed the most time.
-- Terminal / administrative stagees are excluded from the ranking so they
-- cannot be mis-identified as bottlenecks:
--   · Backordered  — external hold, not a process stage
--   · Cancelled    — order did not complete
--   · Delivered    — final state, has no successor duration
-- NULL durations (terminal log rows) are also excluded for the same reason.
-- =============================================================================
ranked_stages AS (
    SELECT
        order_id,
        from_stage                                         AS stage,
        hours_in_stage,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY     hours_in_stage DESC
        )                                                   AS stage_rank

    FROM stage_intervals
    WHERE
        from_stage NOT IN ('Backordered', 'Cancelled', 'Delivered')
        AND hours_in_stage IS NOT NULL
),

-- =============================================================================
-- CTE 4 · bottleneck_stages
-- -----------------------------------------------------------------------------
-- Filters ranked_stages to keep only the top-ranked row per order (stage_rank
-- = 1).  This gives us exactly one bottleneck_stage per order — the single
-- fulfillment stage that caused the longest delay for that particular order.
-- =============================================================================
bottleneck_stages AS (
    SELECT
        order_id,
        stage                                               AS bottleneck_stage,
        hours_in_stage                                     AS bottleneck_hours

    FROM ranked_stages
    WHERE stage_rank = 1
),

-- =============================================================================
-- CTE 5 · order_metrics
-- -----------------------------------------------------------------------------
-- Computes end-to-end elapsed-time KPIs and evaluates SLA compliance per order.
--
-- Elapsed-time metrics (all in hours):
--   time_to_confirm_hours  : order_date  → confirmed_at
--   time_to_ship_hours     : order_date  → shipped_at
--   time_to_deliver_hours  : order_date  → delivered_at (NULL if not yet delivered)
--
-- SLA thresholds (time_to_ship_hours converted to days):
--   Critical   : breached if shipped later than 1.0 day  after order_date
--   Expedited  : breached if shipped later than 2.0 days after order_date
--   Standard   : breached if shipped later than 3.0 days after order_date
--
-- sla_stage : 'On-Time' | 'Breached'
-- =============================================================================
order_metrics AS (
    SELECT
        o.order_id,

        -- Hours from order placement to first Confirmed event
        ROUND(
            (JULIANDAY(tl.confirmed_at) - JULIANDAY(o.order_date)) * 24.0,
            2
        )                                                   AS time_to_confirm_hours,

        -- Hours from order placement to first Shipped event
        ROUND(
            (JULIANDAY(tl.shipped_at) - JULIANDAY(o.order_date)) * 24.0,
            2
        )                                                   AS time_to_ship_hours,

        -- Hours from order placement to Delivered (NULL when not yet delivered)
        ROUND(
            (JULIANDAY(tl.delivered_at) - JULIANDAY(o.order_date)) * 24.0,
            2
        )                                                   AS time_to_deliver_hours,

        -- SLA evaluation: does shipping time exceed the priority-specific threshold?
        CASE
            WHEN o.priority = 'Critical'
                 AND (JULIANDAY(tl.shipped_at) - JULIANDAY(o.order_date)) > 1.0
                THEN 'Breached'
            WHEN o.priority = 'Expedited'
                 AND (JULIANDAY(tl.shipped_at) - JULIANDAY(o.order_date)) > 2.0
                THEN 'Breached'
            WHEN o.priority = 'Standard'
                 AND (JULIANDAY(tl.shipped_at) - JULIANDAY(o.order_date)) > 3.0
                THEN 'Breached'
            -- Order not yet shipped: SLA cannot be evaluated
            WHEN tl.shipped_at IS NULL
                THEN NULL
            ELSE 'On-Time'
        END                                                 AS sla_stage

    FROM raw_orders         o
    JOIN order_timeline     tl USING (order_id)
)

-- =============================================================================
-- FINAL SELECT
-- -----------------------------------------------------------------------------
-- Joins all CTEs back to raw_orders and produces one enriched fact row per
-- order.  Cancelled orders are excluded — they never reached a deliverable
-- outcome and would skew SLA and delivery-time aggregations.
--
-- Output columns:
--   order_id, buyer_name, region, fulfillment_team, priority,
--   product_category, order_date, order_value_usd, total_units,
--   confirmed_at, shipped_at, delivered_at,
--   time_to_confirm_hours, time_to_ship_hours, time_to_deliver_hours,
--   sla_stage, sla_breached (integer: 1 = Breached, 0 = On-Time, NULL = unknown),
--   was_backordered, bottleneck_stage, bottleneck_hours
-- =============================================================================
SELECT
    -- ── Order identity ──────────────────────────────────────────────────────
    o.order_id,
    o.buyer_name,
    o.region,
    o.fulfillment_team,
    o.priority,
    o.product_category,
    o.order_date,
    o.order_value_usd,
    o.total_units,

    -- ── Key milestone timestamps ─────────────────────────────────────────────
    tl.confirmed_at,
    tl.shipped_at,
    tl.delivered_at,

    -- ── Elapsed-time KPIs ────────────────────────────────────────────────────
    om.time_to_confirm_hours,
    om.time_to_ship_hours,
    om.time_to_deliver_hours,

    -- ── SLA evaluation ───────────────────────────────────────────────────────
    om.sla_stage,
    CASE om.sla_stage
        WHEN 'Breached'  THEN 1
        WHEN 'On-Time'   THEN 0
        ELSE                  NULL   -- shipped_at IS NULL → indeterminate
    END                                                     AS sla_breached,

    -- ── Special-event flags ──────────────────────────────────────────────────
    tl.was_backordered,

    -- ── Pipeline bottleneck ──────────────────────────────────────────────────
    bs.bottleneck_stage,
    bs.bottleneck_hours

FROM raw_orders             o
JOIN order_timeline         tl USING (order_id)
JOIN order_metrics          om USING (order_id)
LEFT JOIN bottleneck_stages bs USING (order_id)

-- Exclude cancelled orders from the fact table
WHERE tl.was_cancelled = 0
