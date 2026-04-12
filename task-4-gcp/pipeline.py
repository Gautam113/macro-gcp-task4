# pipeline.py
import os
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    WorkerOptions,
    SetupOptions
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

from config import (PROJECT_ID, REGION, JOB_NAME, MACHINE_TYPE,
                    MAX_WORKERS, NUM_WORKERS, TEMP_LOCATION,
                    STAGING_LOCATION, NODES_CSV, READINGS_CSV, BQ_TABLE_ID)
from enrich    import enrich_readings, make_group_key
from aggregate import aggregate_group
from schema    import REGIONAL_SUMMARY_SCHEMA

logging.basicConfig(level=logging.INFO)


# ── Module-level wrapper functions ──────────────────────────────────────────
# Imports are done INSIDE the function body so they resolve correctly
# on remote Dataflow workers (avoids lambda pickling issues).

def _parse_node(line):
    from parse import parse_node_line
    result = parse_node_line(line)
    return [result] if result is not None else []


def _parse_reading(line):
    from parse import parse_reading_line
    result = parse_reading_line(line)
    return [result] if result is not None else []


# ── Pipeline ─────────────────────────────────────────────────────────────────

def run(runner="DataflowRunner"):

    options = PipelineOptions()

    # Google Cloud options
    gcloud = options.view_as(GoogleCloudOptions)
    gcloud.project          = PROJECT_ID
    gcloud.region           = REGION
    gcloud.job_name         = JOB_NAME
    gcloud.temp_location    = TEMP_LOCATION
    gcloud.staging_location = STAGING_LOCATION

    # Runner
    options.view_as(StandardOptions).runner = runner

    # Worker options
    worker = options.view_as(WorkerOptions)
    worker.machine_type    = MACHINE_TYPE
    worker.num_workers     = NUM_WORKERS
    worker.max_num_workers = MAX_WORKERS

    # Package local modules and ship them to Dataflow workers
    setup = options.view_as(SetupOptions)
    setup.save_main_session = True
    setup.setup_file = os.path.abspath('./setup.py')

    logging.info(f"Starting pipeline | Runner: {runner}")

    with beam.Pipeline(options=options) as pipeline:

        # STEP 1 — Read grid_nodes.csv
        nodes = (
            pipeline
            | "ReadNodes"  >> beam.io.ReadFromText(NODES_CSV, skip_header_lines=1)
            | "ParseNodes" >> beam.FlatMap(_parse_node)
        )

        # STEP 2 — Read power_consumption.csv
        readings = (
            pipeline
            | "ReadReadings"  >> beam.io.ReadFromText(READINGS_CSV, skip_header_lines=1)
            | "ParseReadings" >> beam.FlatMap(_parse_reading)
        )

        # STEP 3 — JOIN on node_id
        joined = (
            {"node": nodes, "readings": readings}
            | "JoinOnNodeId" >> beam.CoGroupByKey()
        )

        # STEP 4 — Attach region/node_type to every reading
        enriched = (
            joined
            | "EnrichReadings" >> beam.FlatMap(enrich_readings)
        )

        # STEP 5 — Create (region, node_type, date) group key
        keyed = (
            enriched
            | "MakeGroupKey" >> beam.Map(make_group_key)
        )

        # STEP 6 — Group readings by key
        grouped = (
            keyed
            | "GroupByKey" >> beam.GroupByKey()
        )

        # STEP 7 — Aggregate 6 metrics per group
        aggregated = (
            grouped
            | "Aggregate" >> beam.FlatMap(aggregate_group)
        )

        # STEP 8 — Write to BigQuery
        aggregated | "WriteToBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE_ID,
            schema=REGIONAL_SUMMARY_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {"type": "DAY", "field": "date"},
                "clustering":       {"fields": ["region", "node_type"]}
            },
            custom_gcs_temp_location=TEMP_LOCATION,
        )

    logging.info("Pipeline completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DataflowRunner")
    args, _ = parser.parse_known_args()
    run(runner=args.runner)