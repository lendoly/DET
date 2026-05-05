# DET Programming Test

## Setup

**Requirements:** [pyenv](https://github.com/pyenv/pyenv) with the `pyenv-virtualenv` plugin.

```bash
pyenv virtualenv 3.13.12 DET
pyenv local DET
pip install -r requirements.txt
```

---

## Part 1 — Device Stream Reader

Reads a newline-delimited JSON file where each line represents a sensor reading.

Each record must have:

| Field | Type | Constraint |
|-------|------|------------|
| `device_id` | string | non-empty |
| `timestamp` | string | valid ISO8601 |
| `metric` | string | non-empty |
| `value` | number | integer or float (no booleans) |

Invalid lines are logged and skipped. Valid records are sorted by `device_id` and kept in memory.

### Run

```bash
# uses sample_stream.txt by default
python part_1.py

# pass a custom file
python part_1.py path/to/file.txt
```

### Example output

```
Loaded 281 valid records.

sensor-01:
  total_readings : 14
  latest_timestamp: 2024-03-15T07:10:01+00:00
  temperature: avg=20.61 (14 readings)
...
```

---

## Part 2 — Device Status API Client

Fetches device status from a REST API with retry logic.

**Endpoint:** `GET {base_url}/devices/{device_id}`  
**Auth:** `X-API-Key` request header

Retries automatically on `429` and `503` responses using exponential backoff. All other errors raise immediately.

### Key components

- `DeviceStatusClient(base_url, api_key, max_retries=3, base_delay=1.0)` — configurable HTTP client
- `DeviceStatus` — dataclass returned by `get_status(device_id)`
- `fetch_all_statuses(device_ids, client)` — bulk fetch, errors captured per device as strings

---

## Tests

```bash
# run all tests
python -m pytest

# run per part
python -m pytest tests/test_part_1.py
python -m pytest tests/test_part_2.py
```

---

## External Libraries

All standard library modules (`json`, `logging`, `datetime`, `argparse`, `time`, `dataclasses`, `unittest.mock`) require no installation. The two third-party dependencies are:

### `requests`

Used in Part 2 to make HTTP calls to the device status API. It provides a simple, human-friendly interface over Python's lower-level `urllib`, handling connection management, headers, JSON decoding, and HTTP error responses out of the box.

In this project it is used to:
- Send `GET` requests with a custom `X-API-Key` header
- Read the JSON response body via `response.json()`
- Raise structured `HTTPError` exceptions via `response.raise_for_status()`

### `pytest`

Used as the test runner for both parts. Compared to the built-in `unittest`, pytest requires less boilerplate (plain `assert` statements instead of `self.assertEqual`), supports powerful fixtures, and produces clearer failure output.

Features used in this project:
- `tmp_path` fixture — provides a temporary directory per test for creating isolated input files without touching the real filesystem
- `caplog` fixture — captures log output during a test, used to assert that invalid records produce an `ERROR` log entry
- `pytest.approx` — floating-point comparison with tolerance, used when asserting computed averages
- `pytest.raises` — context manager that asserts a specific exception is raised
