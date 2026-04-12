"""
grid_producer.py
----------------
Reads power_consumption.csv and replays rows as JSON events to Azure Event Hub.
Runs as a Kubernetes Deployment; uses environment variables for configuration.
Exposes /health and /ready HTTP endpoints for Kubernetes probes.
"""

import os
import csv
import json
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch

# ── Configuration ──────────────────────────────────────────────────────────────
EVENT_HUB_CONNECTION_STRING = os.environ["EVENT_HUB_CONNECTION_STRING"]
EVENT_HUB_NAME              = os.environ.get("EVENT_HUB_NAME", "grid-events-hub")
PUBLISH_BATCH_SIZE          = int(os.environ.get("PUBLISH_BATCH_SIZE", "100"))
REPLAY_LOOP                 = os.environ.get("REPLAY_LOOP", "true").lower() == "true"
DATA_FILE                   = os.environ.get("DATA_FILE", "power_consumption.csv")
PUBLISH_INTERVAL_SECONDS    = float(os.environ.get("PUBLISH_INTERVAL_SECONDS", "1.0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Health check HTTP server ────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/ready"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # suppress access logs
        pass

def start_health_server(port: int = 8080):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health server listening on :%d", port)

# ── Producer logic ──────────────────────────────────────────────────────────────
def load_readings(filepath: str) -> list[dict]:
    with open(filepath, newline="") as f:
        return list(csv.DictReader(f))


def send_batch(producer: EventHubProducerClient, rows: list[dict]) -> int:
    """
    Send a list of row dicts as a single EventDataBatch.
    Uses node_id as the partition key so all readings from the same node
    go to the same partition — preserving per-node ordering within a pod.
    Returns number of events sent.
    """
    sent = 0
    # Group by partition key (node_id) to respect Event Hub ordering guarantee
    by_node: dict[str, list[dict]] = {}
    for row in rows:
        by_node.setdefault(row["node_id"], []).append(row)

    for node_id, node_rows in by_node.items():
        batch: EventDataBatch = producer.create_batch(partition_key=node_id)
        for row in node_rows:
            payload = json.dumps(row).encode("utf-8")
            try:
                batch.add(EventData(payload))
            except ValueError:
                # Batch full — send and start a new one
                producer.send_batch(batch)
                sent += len(batch)
                batch = producer.create_batch(partition_key=node_id)
                batch.add(EventData(payload))
        producer.send_batch(batch)
        sent += len(batch)

    return sent


def run_producer():
    logger.info("Loading data from %s", DATA_FILE)
    rows = load_readings(DATA_FILE)
    logger.info("Loaded %d readings. Batch size=%d, Replay=%s",
                len(rows), PUBLISH_BATCH_SIZE, REPLAY_LOOP)

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )

    iteration = 0
    with producer:
        while True:
            iteration += 1
            total_sent = 0
            for i in range(0, len(rows), PUBLISH_BATCH_SIZE):
                batch_rows = rows[i : i + PUBLISH_BATCH_SIZE]
                sent = send_batch(producer, batch_rows)
                total_sent += sent
                time.sleep(PUBLISH_INTERVAL_SECONDS)

            logger.info("Iteration %d complete — sent %d events", iteration, total_sent)

            if not REPLAY_LOOP:
                logger.info("REPLAY_LOOP=false — producer exiting.")
                break


if __name__ == "__main__":
    start_health_server()
    run_producer()
