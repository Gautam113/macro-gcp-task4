USE DATABASE ENERGYGRID;
USE SCHEMA OPS;

-- 1. Create Stage pointing specifically to the grid_readings folder
-- Note: Added the full path to the URL to make loading cleaner
CREATE OR REPLACE STAGE adls_synapse_stage
    URL = 'azure://energygridadls1.blob.core.windows.net/synapsefiles/snowflake-export/grid_readings/'
    CREDENTIALS = (
        AZURE_SAS_TOKEN = 'sv=2025-11-05&ss=bfqt&srt=co&sp=rwdlacupyx&se=2027-04-11T12:13:03Z&st=2026-04-11T03:58:03Z&spr=https&sig=OINxc%2BANSEX0%2FWkFdiw1rTtWNa94VYqzB1YHievYMhM%3D'
    );

-- 2. Create the table with corrected data types for boolean flags
CREATE OR REPLACE TABLE POWER_CONSUMPTION_SYNAPSE (
    event_time        TIMESTAMP_NTZ, -- Column 1
    reading_id        VARCHAR(50),   -- Column 2
    node_id           VARCHAR(20),   -- Column 3
    reading_timestamp TIMESTAMP_NTZ, -- Column 4
    consumption_mwh   DECIMAL(18,4), -- Column 5
    generation_mwh    DECIMAL(18,4), -- Column 6
    net_flow_mwh      DECIMAL(18,4), -- Column 7
    voltage_kv        DECIMAL(10,3), -- Column 8
    frequency_hz      DECIMAL(10,3), -- Column 9
    power_factor      DECIMAL(5,3),  -- Column 10
    temperature_c     DECIMAL(6,2),  -- Column 11
    is_peak_demand    BOOLEAN,       -- Column 12
    outage_flag       BOOLEAN,       -- Column 13
    region            VARCHAR(50),   -- Column 14
    node_type         VARCHAR(50)    -- Column 15
);

-- 3. The Corrected COPY INTO Command
COPY INTO POWER_CONSUMPTION_SYNAPSE
FROM (
  SELECT 
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
  FROM @adls_synapse_stage
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1                 -- Back to 1 because your sample has headers
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'ABORT_STATEMENT';

-- 4. Final Verification
SELECT * FROM POWER_CONSUMPTION_SYNAPSE ;
SELECT COUNT(*) FROM POWER_CONSUMPTION_SYNAPSE;


------
-- Create separate stage pointing to where grid_nodes.csv is
-- Change the URL to your energy-data folder
CREATE OR REPLACE STAGE adls_reference_stage
    URL = 'azure://energygridadls1.blob.core.windows.net/synapsefiles/'
    CREDENTIALS = (
        AZURE_SAS_TOKEN = 'sv=2025-11-05&ss=bfqt&srt=co&sp=rwdlacupyx&se=2027-04-11T12:13:03Z&st=2026-04-11T03:58:03Z&spr=https&sig=OINxc%2BANSEX0%2FWkFdiw1rTtWNa94VYqzB1YHievYMhM%3D'
    );

-- Check if grid_nodes.csv is visible
LIST @adls_reference_stage;

CREATE OR REPLACE TABLE GRID_NODES (
    node_id              VARCHAR(20),
    node_name            VARCHAR(100),
    node_type            VARCHAR(50),
    region               VARCHAR(50),
    state                VARCHAR(5),
    capacity_mw          DECIMAL(10,2),
    is_renewable         BOOLEAN,
    commission_year      INTEGER,
    maintenance_schedule VARCHAR(50),
    lat                  DECIMAL(9,6),
    lon                  DECIMAL(9,6)
);

COPY INTO GRID_NODES
FROM @adls_reference_stage/grid_nodes.csv
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = ','
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) FROM GRID_NODES;
-- Expected: 50

-- Create deduplicated view using QUALIFY
-- Keeps only the latest event_time record per reading_id
CREATE OR REPLACE TABLE POWER_CONSUMPTION_CLEAN AS
SELECT
    reading_id,
    node_id,
    reading_timestamp,
    consumption_mwh,
    generation_mwh,
    net_flow_mwh,
    voltage_kv,
    frequency_hz,
    power_factor,
    temperature_c,
    is_peak_demand,
    outage_flag,
    region,
    node_type
FROM POWER_CONSUMPTION_SYNAPSE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY reading_id
    ORDER BY event_time DESC
) = 1;

-- Verify deduplication
SELECT COUNT(*) FROM POWER_CONSUMPTION_CLEAN;
-- Expected: ~5000 (original unique readings)

--DELIVERABLES
--5.1 (b)
-- Create materialised view on clean deduplicated data
CREATE OR REPLACE MATERIALIZED VIEW HOURLY_NODE_SUMMARY AS
SELECT
    node_id,
    node_type,
    region,
    DATE_TRUNC('HOUR', reading_timestamp)       AS hour_bucket,
    SUM(consumption_mwh)                        AS total_consumption_mwh,
    SUM(generation_mwh)                         AS total_generation_mwh,
    SUM(net_flow_mwh)                           AS total_net_flow_mwh,
    AVG(power_factor)                           AS avg_power_factor,
    AVG(voltage_kv)                             AS avg_voltage_kv,
    AVG(frequency_hz)                           AS avg_frequency_hz,
    COUNT(*)                                    AS reading_count,
    SUM(CASE WHEN outage_flag = TRUE
             THEN 1 ELSE 0 END)                 AS outage_readings
FROM POWER_CONSUMPTION_CLEAN
GROUP BY
    node_id,
    node_type,
    region,
    DATE_TRUNC('HOUR', reading_timestamp);

-- Verify
SELECT * FROM HOURLY_NODE_SUMMARY LIMIT 10;
SELECT COUNT(*) FROM HOURLY_NODE_SUMMARY;

-- 5.2 (a)
-- Flags anomalous readings more than 3 std devs from node mean
-- Uses POWER_CONSUMPTION_CLEAN for accurate results
WITH node_stats AS (
    SELECT
        node_id,
        AVG(consumption_mwh)    AS mean_consumption,
        STDDEV(consumption_mwh) AS std_consumption
    FROM POWER_CONSUMPTION_CLEAN
    GROUP BY node_id
)
SELECT
    p.reading_id,
    p.node_id,
    p.node_type,
    p.region,
    p.reading_timestamp,
    ROUND(p.consumption_mwh, 3)                 AS consumption_mwh,
    ROUND(s.mean_consumption, 3)                AS mean_consumption,
    ROUND(s.std_consumption, 3)                 AS std_consumption,
    ROUND(
        (p.consumption_mwh - s.mean_consumption)
        / NULLIF(s.std_consumption, 0)
    , 2)                                        AS z_score
FROM  POWER_CONSUMPTION_CLEAN   p
JOIN  node_stats                 s
    ON p.node_id = s.node_id
WHERE ABS(p.consumption_mwh - s.mean_consumption)
    > 3 * s.std_consumption
ORDER BY
    ABS((p.consumption_mwh - s.mean_consumption)
        / NULLIF(s.std_consumption, 0)) DESC;


-- 5.2 (b)
-- Detects sudden demand spikes using LAG window function
-- Flags readings where change > 50% of previous hour value
WITH hourly_changes AS (
    SELECT
        reading_id,
        node_id,
        node_type,
        region,
        reading_timestamp,
        consumption_mwh,
        LAG(consumption_mwh) OVER (
            PARTITION BY node_id
            ORDER BY reading_timestamp
        )                                       AS prev_hour_consumption,
        consumption_mwh
        - LAG(consumption_mwh) OVER (
            PARTITION BY node_id
            ORDER BY reading_timestamp
          )                                     AS hour_delta_mwh
    FROM POWER_CONSUMPTION_CLEAN
)
SELECT
    reading_id,
    node_id,
    node_type,
    region,
    reading_timestamp,
    ROUND(consumption_mwh, 3)                   AS consumption_mwh,
    ROUND(prev_hour_consumption, 3)             AS prev_hour_consumption,
    ROUND(hour_delta_mwh, 3)                    AS hour_delta_mwh,
    ROUND(
        hour_delta_mwh
        / NULLIF(prev_hour_consumption, 0) * 100
    , 2)                                        AS pct_change
FROM  hourly_changes
WHERE prev_hour_consumption IS NOT NULL
  AND ABS(hour_delta_mwh)
      > 0.50 * ABS(prev_hour_consumption)
ORDER BY ABS(pct_change) DESC;

--5.2 (c)
-- 30-day moving average of daily generation per region
-- Uses region directly from POWER_CONSUMPTION_CLEAN
-- No JOIN needed since region is already in the table
WITH daily_generation AS (
    SELECT
        region,
        CAST(reading_timestamp AS DATE)         AS reading_date,
        SUM(generation_mwh)                     AS daily_generation_mwh
    FROM POWER_CONSUMPTION_CLEAN
    WHERE region IS NOT NULL
    GROUP BY
        region,
        CAST(reading_timestamp AS DATE)
)
SELECT
    region,
    reading_date,
    ROUND(daily_generation_mwh, 2)              AS daily_generation_mwh,
    ROUND(
        AVG(daily_generation_mwh) OVER (
            PARTITION BY region
            ORDER BY reading_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )
    , 2)                                        AS moving_avg_30day_mwh,
    COUNT(*) OVER (
        PARTITION BY region
        ORDER BY reading_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                           AS days_in_window
FROM  daily_generation
ORDER BY region, reading_date;

--5.2 (d)
-- Identifies nodes running below 20% of rated capacity
-- Only query needing GRID_NODES for capacity_mw and node_name
SELECT
    p.node_id,
    n.node_name,
    p.node_type,
    p.region,
    n.capacity_mw,
    ROUND(AVG(p.consumption_mwh), 3)            AS avg_consumption_mwh,
    ROUND(
        AVG(p.consumption_mwh)
        / NULLIF(n.capacity_mw, 0) * 100
    , 2)                                        AS utilisation_pct
FROM  POWER_CONSUMPTION_CLEAN    p
JOIN  GRID_NODES                  n
    ON p.node_id = n.node_id
GROUP BY
    p.node_id,
    n.node_name,
    p.node_type,
    p.region,
    n.capacity_mw
HAVING
    AVG(p.consumption_mwh)
    / NULLIF(n.capacity_mw, 0) * 100 < 20
ORDER BY utilisation_pct ASC;


--Summary (not needed in deliverables)
-- Master summary showing all tables and their sources
SELECT
    'POWER_CONSUMPTION_SYNAPSE'  AS table_name,
    COUNT(*)                     AS total_rows,
    'Azure Synapse → ADLS → Snowflake' AS pipeline_source
FROM POWER_CONSUMPTION_SYNAPSE
UNION ALL
SELECT
    'POWER_CONSUMPTION_CLEAN (view)',
    COUNT(*),
    'Deduplicated view of Synapse data'
FROM POWER_CONSUMPTION_CLEAN
UNION ALL
SELECT
    'HOURLY_NODE_SUMMARY (mat.view)',
    COUNT(*),
    'Pre-aggregated from clean data'
FROM HOURLY_NODE_SUMMARY
UNION ALL
SELECT
    'GRID_NODES',
    COUNT(*),
    'ADLS Gen2 reference file'
FROM GRID_NODES;

--Modified 5.2 (a) for 2 standanrd deviation, to get data
-- Modified 5.2a — using 2.0 sigma threshold
-- More appropriate for datasets with moderate variance
WITH node_stats AS (
    SELECT
        node_id,
        AVG(consumption_mwh)    AS mean_consumption,
        STDDEV(consumption_mwh) AS std_consumption
    FROM POWER_CONSUMPTION_CLEAN
    GROUP BY node_id
)
SELECT
    p.reading_id,
    p.node_id,
    p.node_type,
    p.region,
    p.reading_timestamp,
    ROUND(p.consumption_mwh, 3)                 AS consumption_mwh,
    ROUND(s.mean_consumption, 3)                AS mean_consumption,
    ROUND(s.std_consumption, 3)                 AS std_consumption,
    ROUND(
        (p.consumption_mwh - s.mean_consumption)
        / NULLIF(s.std_consumption, 0)
    , 2)                                        AS z_score,
    CASE
        WHEN ABS((p.consumption_mwh - s.mean_consumption)
             / NULLIF(s.std_consumption, 0)) > 3
             THEN 'CRITICAL — 3 sigma'
        WHEN ABS((p.consumption_mwh - s.mean_consumption)
             / NULLIF(s.std_consumption, 0)) > 2
             THEN 'HIGH — 2 sigma'
        ELSE 'MODERATE — 1.5 sigma'
    END                                         AS anomaly_severity
FROM  POWER_CONSUMPTION_CLEAN   p
JOIN  node_stats                 s
    ON p.node_id = s.node_id
WHERE ABS(p.consumption_mwh - s.mean_consumption)
    > 1.5 * s.std_consumption    -- ← changed from 3 to 1.5
ORDER BY
    ABS((p.consumption_mwh - s.mean_consumption)
        / NULLIF(s.std_consumption, 0)) DESC;


--Modified 5.2 (d) for 50 %


SELECT
    p.node_id,
    n.node_name,
    p.node_type,
    p.region,
    n.capacity_mw,
    ROUND(AVG(p.consumption_mwh), 3)            AS avg_consumption_mwh,
    ROUND(
        AVG(p.consumption_mwh)
        / NULLIF(n.capacity_mw, 0) * 100
    , 2)                                        AS utilisation_pct
FROM  POWER_CONSUMPTION_CLEAN    p
JOIN  GRID_NODES                  n
    ON p.node_id = n.node_id
GROUP BY
    p.node_id,
    n.node_name,
    p.node_type,
    p.region,
    n.capacity_mw
HAVING
    AVG(p.consumption_mwh)
    / NULLIF(n.capacity_mw, 0) * 100 < 50
ORDER BY utilisation_pct ASC;

 
