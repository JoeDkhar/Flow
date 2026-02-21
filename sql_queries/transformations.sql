-- transformations.sql
-- Stage 1: raw ingestion check + time-in-status skeleton
-- TODO: add SLA logic, bottleneck detection

-- CTE 1: rank log entries per order chronologically
WITH ranked_logs AS (
    SELECT
        log_id,
        order_id,
        status,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp) AS rn
    FROM raw_logs
),

-- CTE 2: use LEAD() to pair each status with the next one
--        so we can calculate how long the order sat in each state
status_intervals AS (
    SELECT
        order_id,
        status                                              AS from_status,
        LEAD(status)    OVER (PARTITION BY order_id ORDER BY timestamp) AS to_status,
        timestamp                                           AS entered_at,
        LEAD(timestamp) OVER (PARTITION BY order_id ORDER BY timestamp) AS exited_at,
        ROUND(
            (JULIANDAY(LEAD(timestamp) OVER (PARTITION BY order_id ORDER BY timestamp))
             - JULIANDAY(timestamp)) * 24,
            2
        )                                                   AS hours_in_status
    FROM raw_logs
)

SELECT * FROM status_intervals WHERE to_status IS NOT NULL;
-- Full fact table build coming in next commit
