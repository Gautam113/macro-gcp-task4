-- Create the database
CREATE DATABASE IF NOT EXISTS ENERGYGRID;
-- Use the database
USE DATABASE ENERGYGRID;
-- Create the schema
CREATE SCHEMA IF NOT EXISTS ENERGYGRID.OPS;
-- Use the schema
USE SCHEMA ENERGYGRID.OPS;
-- Confirm
SHOW SCHEMAS IN DATABASE ENERGYGRID;

USE SCHEMA ENERGYGRID.OPS;
CREATE OR REPLACE TABLE GRID_NODES (
 node_id VARCHAR(10) NOT NULL PRIMARY KEY,
 node_name VARCHAR(100),
 node_type VARCHAR(50),
 region VARCHAR(50),
 state VARCHAR(5),
 capacity_mw DECIMAL(10,2),
 is_renewable BOOLEAN,
 commission_year INTEGER,
 maintenance_schedule VARCHAR(50),
 lat DECIMAL(10,6),
 lon DECIMAL(10,6)
);
-- Confirm table was created
DESCRIBE TABLE GRID_NODES;

CREATE OR REPLACE TABLE POWER_CONSUMPTION (
 reading_id VARCHAR(20) NOT NULL PRIMARY KEY,
 node_id VARCHAR(10),
 timestamp TIMESTAMP_NTZ,
 consumption_mwh DECIMAL(12,4),
 generation_mwh DECIMAL(12,4),
 net_flow_mwh DECIMAL(12,4),
 voltage_kv DECIMAL(8,3),
 frequency_hz DECIMAL(6,3),
 power_factor DECIMAL(5,3),
 temperature_c DECIMAL(6,2),
 is_peak_demand BOOLEAN,
 outage_flag BOOLEAN
);
DESCRIBE TABLE POWER_CONSUMPTION;

CREATE OR REPLACE TABLE ALERTS (
 alert_id VARCHAR(10) NOT NULL PRIMARY KEY,
 node_id VARCHAR(10),
 alert_type VARCHAR(50),
 severity VARCHAR(20),
 alert_timestamp TIMESTAMP_NTZ,
 resolution_timestamp TIMESTAMP_NTZ,
 is_resolved BOOLEAN,
 affected_mw DECIMAL(10,2),
 resolution_action VARCHAR(50)
);
DESCRIBE TABLE ALERTS;

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION GCS_SNOWFLAKE_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-energy-macro-project-a3t7/data/');

DESC INTEGRATION GCS_SNOWFLAKE_INT;

USE SCHEMA ENERGYGRID.OPS;

CREATE OR REPLACE STAGE ENERGYGRID.OPS.GCS_ENERGY_STAGE
    URL = 'gcs://snowflake-energy-macro-project-a3t7/data/'
    STORAGE_INTEGRATION = GCS_SNOWFLAKE_INT
    FILE_FORMAT = (
        TYPE                         = 'CSV'
        FIELD_DELIMITER              = ','
        SKIP_HEADER                  = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF                      = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL          = TRUE
        TIMESTAMP_FORMAT             = 'YYYY-MM-DDTHH24:MI:SSZ'
    );
-- Test the stage -- should list your 3 CSV files
LIST @ENERGYGRID.OPS.GCS_ENERGY_STAGE;

USE SCHEMA ENERGYGRID.OPS;

-- Load grid_nodes.csv into GRID_NODES table
COPY INTO GRID_NODES
FROM @GCS_ENERGY_STAGE/grid_nodes.csv
ON_ERROR = 'CONTINUE';

-- Verify: should show 50 rows
SELECT COUNT(*) AS total_rows FROM GRID_NODES;
SELECT * FROM GRID_NODES LIMIT 5;

-- Load power_consumption.csv into POWER_CONSUMPTION table
COPY INTO POWER_CONSUMPTION
FROM @GCS_ENERGY_STAGE/power_consumption.csv
ON_ERROR = 'CONTINUE';
-- Verify: should show 5,000 rows
SELECT COUNT(*) AS total_rows FROM POWER_CONSUMPTION;
SELECT * FROM POWER_CONSUMPTION LIMIT 5;

-- Load alerts.csv into ALERTS table
COPY INTO ALERTS
FROM @GCS_ENERGY_STAGE/alerts.csv
ON_ERROR = 'CONTINUE';
-- Verify: should show 500 rows
SELECT COUNT(*) AS total_rows FROM ALERTS;
SELECT * FROM ALERTS LIMIT 5;

 -- Quick summary of all 3 tables
SELECT 'GRID_NODES' AS table_name, COUNT(*) AS row_count FROM GRID_NODES
UNION ALL
SELECT 'POWER_CONSUMPTION' AS table_name, COUNT(*) AS row_count FROM POWER_CONSUMPTION
UNION ALL
SELECT 'ALERTS' AS table_name, COUNT(*) AS row_count FROM ALERTS;

-- MATERIALIZED View
USE SCHEMA ENERGYGRID.OPS;
CREATE OR REPLACE MATERIALIZED VIEW HOURLY_NODE_SUMMARY AS
SELECT
 node_id,
 DATE_TRUNC('hour', timestamp) AS hour,
 SUM(consumption_mwh) AS total_consumption_mwh,
 SUM(generation_mwh) AS total_generation_mwh,
 AVG(voltage_kv) AS avg_voltage_kv,
 AVG(power_factor) AS avg_power_factor,
 COUNT_IF(outage_flag = TRUE) AS outage_count,
 COUNT_IF(is_peak_demand = TRUE) AS peak_demand_count,
 COUNT(*) AS reading_count
FROM POWER_CONSUMPTION
GROUP BY node_id, DATE_TRUNC('hour', timestamp);
-- Verify it was created
SHOW MATERIALIZED VIEWS IN SCHEMA ENERGYGRID.OPS;
-- Query it (instant -- uses pre-computed result)
SELECT * FROM HOURLY_NODE_SUMMARY LIMIT 10;

-- consumption_mwh is more than 3 standard deviations from the node's own historical mean.

SELECT
 reading_id,
 node_id,
 timestamp,
 consumption_mwh,
 ROUND(AVG(consumption_mwh) OVER (PARTITION BY node_id), 4) AS node_mean,
 ROUND(STDDEV(consumption_mwh) OVER (PARTITION BY node_id), 4) AS node_std,
 ROUND(
 ABS(consumption_mwh - AVG(consumption_mwh) OVER (PARTITION BY node_id))
 / NULLIF(STDDEV(consumption_mwh) OVER (PARTITION BY node_id), 0),
 2) AS z_score
FROM POWER_CONSUMPTION
QUALIFY
 ABS(consumption_mwh - AVG(consumption_mwh) OVER (PARTITION BY node_id))
 > 3 * STDDEV(consumption_mwh) OVER (PARTITION BY node_id)
ORDER BY z_score DESC;

-- Confirm query runs correctly even with 0 results:
SELECT COUNT(*) AS anomaly_count
FROM (
 SELECT
 consumption_mwh,
 AVG(consumption_mwh) OVER (PARTITION BY node_id) AS node_mean,
 STDDEV(consumption_mwh) OVER (PARTITION BY node_id) AS node_std
 FROM POWER_CONSUMPTION
) sub
WHERE ABS(consumption_mwh - node_mean) > 3 * node_std;
-- Expected: 0 (all readings within 3 sigma of their node's mean)


-- Identify readings where the change exceeds 50% of the previous hour's value.

SELECT
 node_id,
 timestamp,
 consumption_mwh AS current_consumption,
 LAG(consumption_mwh) OVER (
 PARTITION BY node_id
 ORDER BY timestamp
 ) AS prev_consumption,
 ROUND(
 ABS(consumption_mwh -
 LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp))
 / NULLIF(LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp), 0)
 * 100,
 1) AS pct_change,
 CASE
 WHEN consumption_mwh >
 LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp) * 1.5
 THEN 'SPIKE UP'
 WHEN consumption_mwh <
 LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp) * 0.5
 THEN 'SPIKE DOWN'
 END AS spike_type
FROM POWER_CONSUMPTION
QUALIFY
 ABS(consumption_mwh -
 LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp))
 > LAG(consumption_mwh) OVER (PARTITION BY node_id ORDER BY timestamp) * 0.5
ORDER BY pct_change DESC
LIMIT 20;


-- 30-day moving average of daily total generation_mwh per region.

WITH daily_generation AS (
 -- Step 1: Aggregate hourly readings to daily totals per region
 SELECT
 n.region,
 DATE_TRUNC('day', p.timestamp) AS day,
 ROUND(SUM(p.generation_mwh), 2) AS daily_generation_mwh
 FROM POWER_CONSUMPTION p
 JOIN GRID_NODES n ON p.node_id = n.node_id
 GROUP BY n.region, DATE_TRUNC('day', p.timestamp)
)
-- Step 2: Apply 30-day rolling average on top of daily totals
SELECT
 region,
 day,
 daily_generation_mwh,
 ROUND(AVG(daily_generation_mwh) OVER (
 PARTITION BY region -- calculate per region independently
 ORDER BY day -- move forward through time
 ROWS BETWEEN 29 PRECEDING -- look back 29 rows
 AND CURRENT ROW -- plus today = 30 days total
 ), 2) AS moving_avg_30d
FROM daily_generation
ORDER BY region, day;

-- a query that calculates the capacity utilisation rate per node: avg(consumption_mwh) / capacity_mw * 100. Identify nodes operating below 20% utilisation on average, which may be candidates for decommissioning.


SELECT
 p.node_id,
 n.node_name,
 n.node_type,
 n.region,
 n.capacity_mw,
 ROUND(AVG(p.consumption_mwh), 4) AS
avg_consumption_mwh,
 ROUND(AVG(p.consumption_mwh) / NULLIF(n.capacity_mw, 0) * 100, 2) AS utilisation_pct,
 CASE
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 20
 THEN 'DECOMMISSION CANDIDATE'
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 50
 THEN 'LOW UTILISATION'
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 80
 THEN 'NORMAL'
 ELSE 'HIGH UTILISATION'
 END AS utilisation_status
FROM POWER_CONSUMPTION p
JOIN GRID_NODES n ON p.node_id = n.node_id
GROUP BY p.node_id, n.node_name, n.node_type, n.region, n.capacity_mw
HAVING utilisation_pct < 20
ORDER BY utilisation_pct ASC;

--> For screenshot

SELECT
 p.node_id,
 n.node_name,
 n.node_type,
 n.region,
 n.capacity_mw,
 ROUND(AVG(p.consumption_mwh), 4) AS
avg_consumption_mwh,
 ROUND(AVG(p.consumption_mwh) / NULLIF(n.capacity_mw, 0) * 100, 2) AS utilisation_pct,
 CASE
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 20 THEN
'DECOMMISSION CANDIDATE'
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 50 THEN 'LOW
UTILISATION'
 WHEN AVG(p.consumption_mwh) / NULLIF(n.capacity_mw,0) * 100 < 80 THEN 'NORMAL'
 ELSE 'HIGH UTILISATION'
 END AS utilisation_status
FROM POWER_CONSUMPTION p
JOIN GRID_NODES n ON p.node_id = n.node_id
GROUP BY p.node_id, n.node_name, n.node_type, n.region, n.capacity_mw
ORDER BY utilisation_pct ASC;
