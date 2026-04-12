# aggregate.py


def aggregate_group(element):
    """
    Receives one group and computes 6 metrics.
    Input:  ((region, node_type, date), [list of reading dicts])
    Output: yields ONE aggregated dict for BigQuery
    """
    group_key, readings_iterable = element
    region, node_type, date = group_key
    readings = list(readings_iterable)

    if not readings:
        return

    total_consumption = sum(r["consumption_mwh"] for r in readings)
    total_generation  = sum(r["generation_mwh"]  for r in readings)
    net_flow          = sum(r["net_flow_mwh"]     for r in readings)
    avg_power_factor  = sum(r["power_factor"]     for r in readings) / len(readings)
    outage_count      = sum(1 for r in readings if r["outage_flag"])
    renewable_gen     = sum(r["generation_mwh"]  for r in readings if r["is_renewable"])

    yield {
        "region":                   region,
        "node_type":                node_type,
        "date":                     date,
        "total_consumption_mwh":    round(total_consumption, 4),
        "total_generation_mwh":     round(total_generation,  4),
        "net_flow_mwh":             round(net_flow,          4),
        "avg_power_factor":         round(avg_power_factor,  6),
        "outage_count":             int(outage_count),
        "renewable_generation_mwh": round(renewable_gen,     4),
        "reading_count":            len(readings),
    }