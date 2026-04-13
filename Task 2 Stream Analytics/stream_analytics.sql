
-- Task 2.1a: Outage and Frequency Deviation Detection

SELECT
    System.Timestamp()                          AS event_time,
    node_id,
    reading_id,
    timestamp                                   AS reading_timestamp,
    outage_flag,
    CAST(frequency_hz AS float)                 AS frequency_hz,
    CAST(voltage_kv AS float)                   AS voltage_kv,
    CAST(consumption_mwh AS float)              AS consumption_mwh,
    CASE
        WHEN CAST(outage_flag AS nvarchar(max)) = 'True'
             THEN 'Outage'
        WHEN CAST(frequency_hz AS float) < 49.8
          OR CAST(frequency_hz AS float) > 50.2
             THEN 'Frequency Deviation'
        ELSE NULL                               -- ← added
    END                                         AS alert_type
INTO   [grid-alerts-output]
FROM   [grid-events-input]
WHERE  CAST(outage_flag AS nvarchar(max)) = 'True'
    OR CAST(frequency_hz AS float) < 49.8
    OR CAST(frequency_hz AS float) > 50.2;



-- Task 2.1b: Sustained Overvoltage (5-min window)


SELECT
    System.Timestamp()                          AS window_end,
    e.node_id,
    n.node_type,
    AVG(CAST(e.voltage_kv AS float))            AS avg_voltage_kv,
    CAST(v.nominal_voltage_kv AS float)         AS nominal_voltage_kv,
    CAST(v.nominal_voltage_kv AS float) * 1.15  AS overvoltage_threshold_kv
INTO   [overvoltage-alerts-output]
FROM   [grid-events-input]        AS e
JOIN   [grid-nodes-ref]         AS n
    ON e.node_id   = n.node_id
JOIN   [nominal-voltages-ref]   AS v
    ON n.node_type = v.node_type
GROUP BY
    e.node_id,
    n.node_type,
    v.nominal_voltage_kv,
    TumblingWindow(minute, 1)
HAVING
    AVG(CAST(e.voltage_kv AS float))
  > CAST(v.nominal_voltage_kv AS float) * 1.15;


-- Task 2.2a: Peak Demand (15-min tumbling window)


SELECT
    System.Timestamp()                          AS window_end,
    n.region,
    SUM(CAST(e.consumption_mwh AS float))       AS total_consumption_mwh,
    CAST(r.total_capacity_mw AS float)          AS total_capacity_mw,
    CAST(r.total_capacity_mw AS float) * 0.80   AS peak_threshold_mwh,
    CASE
        WHEN SUM(CAST(e.consumption_mwh AS float))
           > CAST(r.total_capacity_mw AS float) * 0.80
        THEN 1 ELSE 0
    END                                         AS is_peak_window
INTO   [peak-demand-output]
FROM   [grid-events-input]        AS e
JOIN   [grid-nodes-ref]         AS n
    ON e.node_id  = n.node_id
JOIN   [region-capacity-ref]    AS r
    ON n.region   = r.region
GROUP BY
    n.region,
    r.total_capacity_mw,
    TumblingWindow(minute, 1)
HAVING
    SUM(CAST(e.consumption_mwh AS float))
  > CAST(r.total_capacity_mw AS float) * 0.80;


-- Task 2.2b: Rolling Average (Hopping Window)

SELECT
    System.Timestamp()                          AS window_end,
    node_id,
    AVG(CAST(consumption_mwh AS float))         AS rolling_avg_consumption_mwh,
    COUNT(*)                                    AS reading_count
INTO   [rolling-avg-output1]
FROM   [grid-events-input]
GROUP BY
    node_id,
    HoppingWindow(minute, 15, 5);


-- Task 2.3a: Renewable Generation Mix (1-hour window)

SELECT
    System.Timestamp()                          AS window_end,
    e.region,
    SUM(CAST(e.generation_mwh AS float))        AS total_generation_mwh,
    SUM(CASE
            WHEN CAST(n.is_renewable AS nvarchar(max)) = 'True'
            THEN CAST(e.generation_mwh AS float)
            ELSE 0                              -- ← already correct
        END)                                    AS renewable_generation_mwh,
    CASE
        WHEN SUM(CAST(e.generation_mwh AS float)) > 0
        THEN ROUND(
            SUM(CASE
                    WHEN CAST(n.is_renewable AS nvarchar(max)) = 'True'
                    THEN CAST(e.generation_mwh AS float)
                    ELSE 0                      -- ← already correct
                END)
            / SUM(CAST(e.generation_mwh AS float)) * 100, 2)
        ELSE 0                                  -- ← added
    END                                         AS renewable_pct
INTO   [adls-renewable-output]
FROM   [grid-events-input]                        AS e
JOIN   [grid-nodes-ref]                         AS n
    ON e.node_id = n.node_id
GROUP BY
    e.region,
    TumblingWindow(minute, 1);


--To Synapse
-- Production Architecture: Event Hub → Stream Analytics → Synapse
-- This sends ALL enriched readings directly to Synapse
SELECT
    System.Timestamp()                      AS event_time,
    e.reading_id,
    e.node_id,
    e.timestamp                             AS reading_timestamp,
    CAST(e.consumption_mwh AS float)        AS consumption_mwh,
    CAST(e.generation_mwh AS float)         AS generation_mwh,
    CAST(e.net_flow_mwh AS float)           AS net_flow_mwh,
    CAST(e.voltage_kv AS float)             AS voltage_kv,
    CAST(e.frequency_hz AS float)           AS frequency_hz,
    CAST(e.power_factor AS float)           AS power_factor,
    CAST(e.temperature_c AS float)          AS temperature_c,
    CAST(e.is_peak_demand AS nvarchar(max)) AS is_peak_demand,
    CAST(e.outage_flag AS nvarchar(max))    AS outage_flag,
    n.region,
    n.node_type
INTO   [synapse-stream-output]
FROM   [grid-events-input]                    AS e
JOIN   [grid-nodes-ref]                     AS n
    ON e.node_id = n.node_id;

