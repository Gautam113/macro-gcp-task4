# enrich.py
import logging


def enrich_readings(element):
    """
    After the JOIN, attaches region/node_type to every reading.
    Input:  (node_id, {"node": [node_dict], "readings": [reading_dict, ...]})
    Output: yields one enriched reading dict per reading
    """
    node_id, grouped = element
    node_list = grouped.get("node", [])
    readings  = grouped.get("readings", [])

    if not node_list:
        logging.warning(f"No node info for {node_id}. Skipping {len(readings)} readings.")
        return

    node = node_list[0]

    for reading in readings:
        reading["node_id"]      = node_id
        reading["region"]       = node["region"]
        reading["node_type"]    = node["node_type"]
        reading["is_renewable"] = node["is_renewable"]
        yield reading


def make_group_key(reading):
    """
    Creates the (region, node_type, date) composite group key.
    Input:  enriched reading dict
    Output: ((region, node_type, date), reading_dict)
    """
    key = (reading["region"], reading["node_type"], reading["date"])
    return (key, reading)