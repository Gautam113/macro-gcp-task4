
CREATE TABLE dbo.grid_readings_stream
(
    event_time        DATETIME2,
    reading_id        VARCHAR(20),
    node_id           VARCHAR(20),
    reading_timestamp VARCHAR(30),
    consumption_mwh   DECIMAL(12,3),
    generation_mwh    DECIMAL(12,3),
    net_flow_mwh      DECIMAL(12,3),
    voltage_kv        DECIMAL(8,3),
    frequency_hz      DECIMAL(6,3),
    power_factor      DECIMAL(5,3),
    temperature_c     DECIMAL(6,2),
    is_peak_demand    VARCHAR(10),
    outage_flag       VARCHAR(10),
    region            VARCHAR(50),
    node_type         VARCHAR(50)
)
WITH
(
    DISTRIBUTION = HASH(node_id),
    CLUSTERED COLUMNSTORE INDEX
);

-- Check live data arriving from Event Hub via ASA
-- SELECT COUNT(*) FROM dbo.grid_readings_stream;

-- -- See latest records
-- SELECT TOP 20 *
-- FROM dbo.grid_readings_stream
-- ORDER BY event_time DESC;

-- -- Check data per region
-- SELECT*                    
-- FROM dbo.grid_readings_stream

-- Task 3.1c — Hourly aggregation on LIVE streaming data
SELECT
    node_id,
    DATEPART(YEAR,  CAST(reading_timestamp AS DATETIME2))  AS year,
    DATEPART(MONTH, CAST(reading_timestamp AS DATETIME2))  AS month,
    DATEPART(DAY,   CAST(reading_timestamp AS DATETIME2))  AS day,
    DATEPART(HOUR,  CAST(reading_timestamp AS DATETIME2))  AS hour,
    AVG(consumption_mwh)    AS avg_consumption_mwh,
    MIN(consumption_mwh)    AS min_consumption_mwh,
    MAX(consumption_mwh)    AS max_consumption_mwh,
    COUNT(*)                AS reading_count
FROM  dbo.grid_readings_stream     
GROUP BY
    node_id,
    DATEPART(YEAR,  CAST(reading_timestamp AS DATETIME2)),
    DATEPART(MONTH, CAST(reading_timestamp AS DATETIME2)),
    DATEPART(DAY,   CAST(reading_timestamp AS DATETIME2)),
    DATEPART(HOUR,  CAST(reading_timestamp AS DATETIME2))
ORDER BY node_id, year, month, day, hour;


-- Task 3.2a: Top 5 nodes by total outage duration


WITH outage_windows AS (
    SELECT
        node_id,
        reading_timestamp,
        
        ROW_NUMBER() OVER (
            PARTITION BY node_id
            ORDER BY CAST(reading_timestamp AS DATETIME2)
        )
        -
        ROW_NUMBER() OVER (
            PARTITION BY node_id, outage_flag
            ORDER BY CAST(reading_timestamp AS DATETIME2)
        )                           AS outage_group
    FROM  dbo.grid_readings_stream
    WHERE outage_flag = 'True'      
),
outage_duration AS (
    SELECT
        node_id,
        outage_group,
        COUNT(*) * 1.0              AS duration_hours,
        COUNT(*)                    AS outage_readings
    FROM  outage_windows
    GROUP BY node_id, outage_group
),
node_totals AS (
    SELECT
        node_id,
        SUM(duration_hours)         AS total_outage_hours,
        COUNT(outage_group)         AS total_outage_events
    FROM  outage_duration
    GROUP BY node_id
)
SELECT TOP 5
    t.node_id,
    n.node_name,
    n.node_type,
    n.region,
    t.total_outage_hours,
    t.total_outage_events
FROM  node_totals        t
JOIN  dbo.grid_nodes     n ON t.node_id = n.node_id
ORDER BY total_outage_hours DESC;


-- Task 3.2b: Daily renewable generation percentage per region


SELECT
    CAST(
        CAST(r.reading_timestamp AS DATETIME2)
    AS DATE)                                    AS reading_date,
    n.region,
    ROUND(SUM(r.generation_mwh), 2)             AS total_generation_mwh,
    ROUND(
        SUM(CASE
                WHEN n.is_renewable = 'True'    -- VARCHAR from CSV load
                THEN r.generation_mwh
                ELSE 0
            END), 2)                            AS renewable_generation_mwh,
    ROUND(
        SUM(CASE
                WHEN n.is_renewable = 'True'
                THEN r.generation_mwh
                ELSE 0
            END)
        / NULLIF(SUM(r.generation_mwh), 0) * 100
    , 2)                                        AS renewable_pct
FROM  dbo.grid_readings_stream  r
JOIN  dbo.grid_nodes     n ON r.node_id = n.node_id
GROUP BY
    CAST(CAST(r.reading_timestamp AS DATETIME2) AS DATE),
    n.region
ORDER BY
    reading_date,
    n.region;


-- Task 3.2c: 7-day rolling average of consumption per node


WITH daily_consumption AS (
    --  Aggregate to daily level per node
    SELECT
        node_id,
        CAST(
            CAST(reading_timestamp AS DATETIME2)
        AS DATE)                AS reading_date,
        SUM(consumption_mwh)    AS daily_consumption_mwh
    FROM  dbo.grid_readings_stream
    GROUP BY
        node_id,
        CAST(CAST(reading_timestamp AS DATETIME2) AS DATE)
),
rolling AS (
    -- Compute 7-day rolling average using window function
    SELECT
        node_id,
        reading_date,
        daily_consumption_mwh,
        AVG(daily_consumption_mwh) OVER (
            PARTITION BY node_id
            ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                       AS rolling_7day_avg_mwh,
        COUNT(*) OVER (
            PARTITION BY node_id
            ORDER BY reading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                       AS days_in_window
    FROM  daily_consumption
),
latest_day AS (
    --  Get most recent date per node
    SELECT
        node_id,
        MAX(reading_date)       AS max_date
    FROM  rolling
    GROUP BY node_id
)
--  Filter nodes where latest > 120% of rolling average
SELECT
    r.node_id,
    n.node_name,
    n.node_type,
    n.region,
    r.reading_date,
    ROUND(r.daily_consumption_mwh, 2)       AS daily_consumption_mwh,
    ROUND(r.rolling_7day_avg_mwh, 2)        AS rolling_7day_avg_mwh,
    r.days_in_window,
    ROUND(
        (r.daily_consumption_mwh - r.rolling_7day_avg_mwh)
        / NULLIF(r.rolling_7day_avg_mwh, 0) * 100
    , 2)                                    AS pct_above_avg
FROM  rolling       r
JOIN  latest_day    l ON r.node_id   = l.node_id
                     AND r.reading_date = l.max_date
JOIN  dbo.grid_nodes n ON r.node_id  = n.node_id
WHERE r.daily_consumption_mwh > r.rolling_7day_avg_mwh * 1.20
ORDER BY pct_above_avg DESC;