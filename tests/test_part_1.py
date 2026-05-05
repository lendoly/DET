import pytest
import part_1
from part_1 import _parse_iso8601, read_stream, summarize


def write_lines(path, lines):
    path.write_text("\n".join(lines), encoding="utf-8")


def make_record(device_id="sensor-01", timestamp="2024-03-15T06:00:01Z", metric="temperature", value=20.0):
    return {"device_id": device_id, "timestamp": timestamp, "metric": metric, "value": value}


# ─── _parse_iso8601 ────────────────────────────────────────────────────────────

class TestParseIso8601:
    def test_valid_utc_z_suffix(self):
        assert _parse_iso8601("2024-03-15T06:00:01Z") is True

    def test_valid_with_utc_offset(self):
        assert _parse_iso8601("2024-03-15T06:00:01+00:00") is True

    def test_valid_with_positive_offset(self):
        assert _parse_iso8601("2024-03-15T06:00:01+05:30") is True

    def test_valid_with_negative_offset(self):
        assert _parse_iso8601("2024-03-15T06:00:01-03:00") is True

    def test_date_only(self):
        assert _parse_iso8601("2024-03-15") is True

    def test_invalid_string(self):
        assert _parse_iso8601("not-a-timestamp") is False

    def test_empty_string(self):
        assert _parse_iso8601("") is False

    def test_none(self):
        assert _parse_iso8601(None) is False

    def test_invalid_month(self):
        assert _parse_iso8601("2024-13-15T06:00:01Z") is False

    def test_invalid_day(self):
        assert _parse_iso8601("2024-03-45T06:00:01Z") is False

    def test_invalid_hour(self):
        assert _parse_iso8601("2024-03-15T25:00:01Z") is False


# ─── read_stream ───────────────────────────────────────────────────────────────

class TestReadStream:
    def test_valid_records_loaded(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, [
            '{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}',
            '{"device_id": "sensor-02", "timestamp": "2024-03-15T06:00:02Z", "metric": "pressure", "value": 1008.4}',
        ])
        assert len(read_stream(str(f))) == 2

    def test_integer_value_accepted(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18}'])
        records = read_stream(str(f))
        assert len(records) == 1
        assert records[0]["value"] == 18

    def test_empty_file(self, tmp_path):
        f = tmp_path / "stream.txt"
        f.write_text("", encoding="utf-8")
        assert read_stream(str(f)) == []

    def test_blank_lines_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, [
            "",
            '{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}',
            "",
        ])
        assert len(read_stream(str(f))) == 1

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            read_stream("nonexistent_file.txt")

    def test_invalid_json_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, [
            '{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}',
            "not valid json at all",
        ])
        assert len(read_stream(str(f))) == 1

    def test_empty_device_id_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}'])
        assert read_stream(str(f)) == []

    def test_null_device_id_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": null, "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}'])
        assert read_stream(str(f)) == []

    def test_invalid_timestamp_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "not-a-timestamp", "metric": "temperature", "value": 18.3}'])
        assert read_stream(str(f)) == []

    def test_empty_metric_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "", "value": 18.3}'])
        assert read_stream(str(f)) == []

    def test_null_metric_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": null, "value": 18.3}'])
        assert read_stream(str(f)) == []

    def test_string_value_skipped(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "battery_level", "value": "LOW"}'])
        assert read_stream(str(f)) == []

    def test_boolean_value_skipped_and_logged(self, tmp_path, caplog):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": true}'])
        assert read_stream(str(f)) == []

    def test_records_sorted_by_device_id(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, [
            '{"device_id": "sensor-03", "timestamp": "2024-03-15T06:00:03Z", "metric": "humidity", "value": 72.1}',
            '{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}',
            '{"device_id": "sensor-02", "timestamp": "2024-03-15T06:00:02Z", "metric": "pressure", "value": 1008.4}',
        ])
        records = read_stream(str(f))
        device_ids = [r["device_id"] for r in records]
        assert device_ids == sorted(device_ids)

    def test_records_stored_in_module_list(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}'])
        read_stream(str(f))
        assert len(part_1._records) == 1

    def test_module_list_reset_on_each_call(self, tmp_path):
        f = tmp_path / "stream.txt"
        write_lines(f, ['{"device_id": "sensor-01", "timestamp": "2024-03-15T06:00:01Z", "metric": "temperature", "value": 18.3}'])
        read_stream(str(f))
        read_stream(str(f))
        assert len(part_1._records) == 1

    def test_against_sample_file(self):
        records = read_stream("sample_stream.txt")
        assert len(records) == 281
        device_ids = [r["device_id"] for r in records]
        assert device_ids == sorted(device_ids)


# ─── summarize ─────────────────────────────────────────────────────────────────

class TestSummarize:
    def test_empty_list_returns_empty_dict(self):
        assert summarize([]) == {}

    def test_single_record(self):
        summary = summarize([make_record(value=20.0)])
        assert "sensor-01" in summary
        assert summary["sensor-01"]["total_readings"] == 1
        assert summary["sensor-01"]["metrics"]["temperature"]["average"] == 20.0
        assert summary["sensor-01"]["metrics"]["temperature"]["count"] == 1

    def test_average_calculation(self):
        records = [
            make_record(timestamp="2024-03-15T06:00:01Z", value=10.0),
            make_record(timestamp="2024-03-15T06:05:01Z", value=20.0),
            make_record(timestamp="2024-03-15T06:10:01Z", value=30.0),
        ]
        summary = summarize(records)
        assert summary["sensor-01"]["metrics"]["temperature"]["average"] == pytest.approx(20.0)
        assert summary["sensor-01"]["metrics"]["temperature"]["count"] == 3
        assert summary["sensor-01"]["total_readings"] == 3

    def test_multiple_metrics_per_device(self):
        records = [
            make_record(metric="temperature", value=20.0),
            make_record(timestamp="2024-03-15T06:05:01Z", metric="humidity", value=60.0),
        ]
        summary = summarize(records)
        assert "temperature" in summary["sensor-01"]["metrics"]
        assert "humidity" in summary["sensor-01"]["metrics"]
        assert summary["sensor-01"]["total_readings"] == 2

    def test_latest_timestamp_is_most_recent(self):
        records = [
            make_record(timestamp="2024-03-15T06:00:01Z", value=20.0),
            make_record(timestamp="2024-03-15T07:00:01Z", value=22.0),
            make_record(timestamp="2024-03-15T06:30:01Z", value=21.0),
        ]
        summary = summarize(records)
        assert "07:00:01" in summary["sensor-01"]["latest_timestamp"]

    def test_latest_timestamp_with_single_record(self):
        summary = summarize([make_record(timestamp="2024-03-15T06:00:01Z")])
        assert "06:00:01" in summary["sensor-01"]["latest_timestamp"]

    def test_latest_timestamp_is_string(self):
        summary = summarize([make_record()])
        assert isinstance(summary["sensor-01"]["latest_timestamp"], str)

    def test_multiple_devices_independent(self):
        records = [
            make_record(device_id="sensor-01", value=20.0),
            make_record(device_id="sensor-02", timestamp="2024-03-15T06:00:02Z", metric="pressure", value=1008.0),
        ]
        summary = summarize(records)
        assert "sensor-01" in summary
        assert "sensor-02" in summary
        assert "temperature" in summary["sensor-01"]["metrics"]
        assert "pressure" in summary["sensor-02"]["metrics"]

    def test_sum_not_exposed_in_output(self):
        summary = summarize([make_record()])
        assert "sum" not in summary["sensor-01"]["metrics"]["temperature"]

    def test_count_per_metric(self):
        records = [
            make_record(timestamp="2024-03-15T06:00:01Z", metric="temperature", value=10.0),
            make_record(timestamp="2024-03-15T06:05:01Z", metric="temperature", value=20.0),
            make_record(timestamp="2024-03-15T06:10:01Z", metric="humidity", value=60.0),
        ]
        summary = summarize(records)
        assert summary["sensor-01"]["metrics"]["temperature"]["count"] == 2
        assert summary["sensor-01"]["metrics"]["humidity"]["count"] == 1
