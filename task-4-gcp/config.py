# config.py

PROJECT_ID = "macro-project-a3t7"
REGION     = "us-central1"
BUCKET     = "macro-bucket-macro-project-a3t7"

# Input CSV files on GCS
NODES_CSV    = f"gs://{BUCKET}/energy/grid_nodes.csv"
READINGS_CSV = f"gs://{BUCKET}/energy/power_consumption.csv"

# Dataflow job settings
TEMP_LOCATION    = f"gs://{BUCKET}/temp"
STAGING_LOCATION = f"gs://{BUCKET}/staging"
JOB_NAME         = "energy-grid-aggregation"
MACHINE_TYPE     = "e2-standard-2"
MAX_WORKERS      = 2
NUM_WORKERS      = 1

# BigQuery output
BQ_DATASET  = "energy_grid"
BQ_TABLE    = "regional_grid_summary"
BQ_TABLE_ID = f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"