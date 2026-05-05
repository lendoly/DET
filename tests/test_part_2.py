import pytest
from unittest.mock import MagicMock, patch, call
from requests import HTTPError

from part_2 import DeviceStatus, DeviceStatusClient, fetch_all_statuses


BASE_URL = "https://api.example.com"
API_KEY = "test-key"
DEVICE_ID = "sensor-42"

SAMPLE_PAYLOAD = {
    "device_id": DEVICE_ID,
    "status": "online",
    "last_seen": "2024-03-15T08:30:00Z",
    "firmware_version": "2.1.4",
}


def make_client(**kwargs) -> DeviceStatusClient:
    return DeviceStatusClient(base_url=BASE_URL, api_key=API_KEY, **kwargs)


def make_response(status_code: int, json_data: dict | None = None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data or {}
    response.raise_for_status.side_effect = (
        HTTPError(f"{status_code} Error") if status_code != 200 else None
    )
    return response


# ─── DeviceStatusClient.get_status ────────────────────────────────────────────

class TestGetStatus:
    @patch("part_2.requests.get")
    def test_success_returns_dataclass(self, mock_get):
        mock_get.return_value = make_response(200, SAMPLE_PAYLOAD)
        client = make_client()

        result = client.get_status(DEVICE_ID)

        assert isinstance(result, DeviceStatus)
        assert result.device_id == "sensor-42"
        assert result.status == "online"
        assert result.last_seen == "2024-03-15T08:30:00Z"
        assert result.firmware_version == "2.1.4"

    @patch("part_2.requests.get")
    def test_request_url_and_header(self, mock_get):
        mock_get.return_value = make_response(200, SAMPLE_PAYLOAD)
        client = make_client()

        client.get_status(DEVICE_ID)

        mock_get.assert_called_once_with(
            f"{BASE_URL}/devices/{DEVICE_ID}",
            headers={"X-API-Key": API_KEY},
        )

    @patch("part_2.requests.get")
    def test_base_url_trailing_slash_stripped(self, mock_get):
        mock_get.return_value = make_response(200, SAMPLE_PAYLOAD)
        client = DeviceStatusClient(base_url=f"{BASE_URL}/", api_key=API_KEY)

        client.get_status(DEVICE_ID)

        url_used = mock_get.call_args[0][0]
        assert "//" not in url_used.replace("https://", "")

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_retries_on_429(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            make_response(429),
            make_response(429),
            make_response(200, SAMPLE_PAYLOAD),
        ]
        client = make_client(max_retries=3, base_delay=1.0)

        result = client.get_status(DEVICE_ID)

        assert isinstance(result, DeviceStatus)
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_retries_on_503(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            make_response(503),
            make_response(200, SAMPLE_PAYLOAD),
        ]
        client = make_client(max_retries=3, base_delay=1.0)

        result = client.get_status(DEVICE_ID)

        assert isinstance(result, DeviceStatus)
        assert mock_get.call_count == 2

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_exponential_backoff_delays(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            make_response(503),
            make_response(503),
            make_response(200, SAMPLE_PAYLOAD),
        ]
        client = make_client(max_retries=3, base_delay=2.0)

        client.get_status(DEVICE_ID)

        mock_sleep.assert_has_calls([call(2.0), call(4.0)])

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_raises_after_max_retries_exhausted(self, mock_get, mock_sleep):
        mock_get.return_value = make_response(503)
        client = make_client(max_retries=2, base_delay=0.0)

        with pytest.raises(HTTPError):
            client.get_status(DEVICE_ID)

        assert mock_get.call_count == 3  # initial + 2 retries

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_raises_immediately_on_404(self, mock_get, mock_sleep):
        mock_get.return_value = make_response(404)
        client = make_client(max_retries=3)

        with pytest.raises(HTTPError):
            client.get_status(DEVICE_ID)

        assert mock_get.call_count == 1
        mock_sleep.assert_not_called()

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_raises_immediately_on_500(self, mock_get, mock_sleep):
        mock_get.return_value = make_response(500)
        client = make_client(max_retries=3)

        with pytest.raises(HTTPError):
            client.get_status(DEVICE_ID)

        assert mock_get.call_count == 1
        mock_sleep.assert_not_called()

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_configurable_max_retries(self, mock_get, mock_sleep):
        mock_get.return_value = make_response(429)
        client = make_client(max_retries=1, base_delay=0.0)

        with pytest.raises(HTTPError):
            client.get_status(DEVICE_ID)

        assert mock_get.call_count == 2  # initial + 1 retry


# ─── fetch_all_statuses ────────────────────────────────────────────────────────

class TestFetchAllStatuses:
    @patch("part_2.requests.get")
    def test_all_successful(self, mock_get):
        mock_get.return_value = make_response(200, SAMPLE_PAYLOAD)
        client = make_client()

        results = fetch_all_statuses(["sensor-42", "sensor-43"], client)

        assert len(results) == 2
        assert all(isinstance(v, DeviceStatus) for v in results.values())

    @patch("part_2.requests.get")
    def test_failed_device_returns_error_string(self, mock_get):
        mock_get.return_value = make_response(404)
        client = make_client()

        results = fetch_all_statuses([DEVICE_ID], client)

        assert isinstance(results[DEVICE_ID], str)
        assert len(results[DEVICE_ID]) > 0

    @patch("part_2.requests.get")
    def test_mixed_success_and_failure(self, mock_get):
        mock_get.side_effect = [
            make_response(200, SAMPLE_PAYLOAD),
            make_response(404),
        ]
        client = make_client()

        results = fetch_all_statuses(["sensor-01", "sensor-02"], client)

        assert isinstance(results["sensor-01"], DeviceStatus)
        assert isinstance(results["sensor-02"], str)

    @patch("part_2.requests.get")
    def test_empty_device_list(self, mock_get):
        client = make_client()

        results = fetch_all_statuses([], client)

        assert results == {}
        mock_get.assert_not_called()

    @patch("part_2.time.sleep")
    @patch("part_2.requests.get")
    def test_retries_exhausted_captured_as_error(self, mock_get, mock_sleep):
        mock_get.return_value = make_response(503)
        client = make_client(max_retries=1, base_delay=0.0)

        results = fetch_all_statuses([DEVICE_ID], client)

        assert isinstance(results[DEVICE_ID], str)
