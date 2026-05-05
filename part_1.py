import argparse
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.ERROR,
    format="%(levelname)s - line %(lineno_hint)s: %(message)s",
)

_records: list[dict] = []


def _parse_iso8601(value: str) -> bool:
    """Return True if value is a valid ISO8601 datetime string, False otherwise."""
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except (AttributeError, ValueError):
        return False


def read_stream(filepath: str = "sample_stream.txt") -> list[dict]:
    """Read a newline-delimited JSON file and return validated records sorted by device_id.

    Each line must be a JSON object with the following fields:
        - device_id (str): non-empty identifier for the device.
        - timestamp (str): valid ISO8601 datetime.
        - metric (str): non-empty metric name.
        - value (int | float): numeric measurement.

    Invalid lines are logged at ERROR level and skipped. Valid records are also
    stored in the module-level ``_records`` list.

    Args:
        filepath: Path to the stream file.

    Returns:
        List of valid record dicts, sorted ascending by device_id.

    Raises:
        FileNotFoundError: If filepath does not exist.
    """
    global _records
    _records = []

    with open(filepath, encoding="utf-8") as f:
        for line_num, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue

            hint = {"lineno_hint": line_num}

            # Attempt to parse JSON
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                logging.error("invalid JSON — %s", exc, extra=hint)
                continue

            device_id = record.get("device_id")
            # device_id must be a non-empty string
            if not device_id:
                logging.error("device_id is empty or null", extra=hint)
                continue

            timestamp = record.get("timestamp")
            # timestamp must be a valid ISO8601 string
            if not _parse_iso8601(timestamp):
                logging.error(
                    "invalid ISO8601 timestamp %r", timestamp, extra=hint
                )
                continue

            metric = record.get("metric")
            # metric must be a non-empty string
            if not metric:
                logging.error("metric is empty or null", extra=hint)
                continue

            value = record.get("value")
            # value must be numeric (int or float)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                logging.error("value %r is not numeric", value, extra=hint)
                continue

            _records.append(record)


    # sort in-place by device_id for efficient grouping
    _records.sort(key=lambda r: r["device_id"])

    return _records


def summarize(records: list[dict]) -> dict:
    """Compute per-device statistics from a list of validated records.

    Args:
        records: List of record dicts as returned by ``read_stream``.

    Returns:
        Dict keyed by device_id, each value containing:
            - total_readings (int): total number of records for the device.
            - latest_timestamp (str): ISO8601 string of the most recent reading.
            - metrics (dict): per-metric dict with ``average`` (float) and
              ``count`` (int).
    """
    summary: dict[str, dict] = {}

    for record in records:
        device_id = record["device_id"]
        metric = record["metric"]
        value = record["value"]
        timestamp = datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))

        if device_id not in summary:
            summary[device_id] = {
                "total_readings": 0,
                "metrics": {},
                "latest_timestamp": timestamp,
            }

        device = summary[device_id]
        device["total_readings"] += 1

        if metric not in device["metrics"]:
            device["metrics"][metric] = {"sum": 0.0, "count": 0, "average": 0.0}

        device["metrics"][metric]["sum"] += value
        device["metrics"][metric]["count"] += 1
        device["metrics"][metric]["average"] = (
            device["metrics"][metric]["sum"] / device["metrics"][metric]["count"]
        )

        if timestamp > device["latest_timestamp"]:
            device["latest_timestamp"] = timestamp

    for device in summary.values():
        device["latest_timestamp"] = device["latest_timestamp"].isoformat()
        for metric in device["metrics"].values():
            del metric["sum"]

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read and validate a device metric stream file.")
    parser.add_argument("file", nargs="?", default="sample_stream.txt", help="Path to the stream file (default: sample_stream.txt)")
    args = parser.parse_args()

    records = read_stream(args.file)
    print(f"Loaded {len(records)} valid records.")

    summary = summarize(records)
    for device_id, stats in summary.items():
        print(f"\n{device_id}:")
        print(f"  total_readings : {stats['total_readings']}")
        print(f"  latest_timestamp: {stats['latest_timestamp']}")
        for metric, m_stats in stats["metrics"].items():
            print(f"  {metric}: avg={m_stats['average']:.2f} ({m_stats['count']} readings)")
