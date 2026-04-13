# parse.py
import csv
import io
import logging


def parse_node_line(line):
    try:
        HEADERS = ["node_id", "node_name", "node_type", "region", "state",
                   "capacity_mw", "is_renewable", "install_year",
                   "maintenance_schedule", "latitude", "longitude"]
        reader = csv.DictReader(io.StringIO(line), fieldnames=HEADERS)
        row = next(reader)
        return (
            row["node_id"],
            {
                "node_type":    row["node_type"],
                "region":       row["region"],
                "capacity_mw":  float(row["capacity_mw"] or 0),
                "is_renewable": row["is_renewable"].strip().lower() == "true",
            }
        )
    except Exception as e:
        logging.warning(f"Skipping bad node line: {e}")
        return None


def parse_reading_line(line):
    try:
        HEADERS = ["reading_id", "node_id", "timestamp", "consumption_mwh",
                   "generation_mwh", "net_flow_mwh", "power_factor",
                   "voltage_kv", "frequency_hz", "outage_flag", "outage_duration_min"]
        reader = csv.DictReader(io.StringIO(line), fieldnames=HEADERS)
        row = next(reader)
        return (
            row["node_id"],
            {
                "reading_id":      row["reading_id"],
                "date":            row["timestamp"][:10],
                "consumption_mwh": float(row["consumption_mwh"] or 0),
                "generation_mwh":  float(row["generation_mwh"]  or 0),
                "net_flow_mwh":    float(row["net_flow_mwh"]     or 0),
                "power_factor":    float(row["power_factor"]     or 0),
                "outage_flag":     row["outage_flag"].strip().lower() == "true",
            }
        )
    except Exception as e:
        logging.warning(f"Skipping bad reading line: {e}")
        return None