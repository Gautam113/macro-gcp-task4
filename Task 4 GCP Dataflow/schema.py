# schema.py

REGIONAL_SUMMARY_SCHEMA = {
    "fields": [
        {"name": "region",                   "type": "STRING",  "mode": "REQUIRED"},
        {"name": "node_type",                "type": "STRING",  "mode": "REQUIRED"},
        {"name": "date",                     "type": "DATE",    "mode": "REQUIRED"},
        {"name": "total_consumption_mwh",    "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "total_generation_mwh",     "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "net_flow_mwh",             "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "avg_power_factor",         "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "outage_count",             "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "renewable_generation_mwh", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "reading_count",            "type": "INTEGER", "mode": "NULLABLE"},
    ]
}