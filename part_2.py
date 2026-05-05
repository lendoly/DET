import time
from dataclasses import dataclass

import requests


@dataclass
class DeviceStatus:
    """Parsed response from the device status API."""

    device_id: str
    status: str
    last_seen: str
    firmware_version: str


class DeviceStatusClient:
    """HTTP client for the device status API with configurable retry logic.

    Args:
        base_url: Base URL of the API (e.g. ``https://api.example.com``).
        api_key: API key sent as the ``X-API-Key`` request header.
        max_retries: Maximum number of retry attempts on retryable errors (429, 503).
        base_delay: Base delay in seconds for exponential backoff. Delay for
            attempt ``n`` is ``base_delay * 2 ** n``.
    """

    _RETRYABLE_STATUSES = {429, 503}

    def __init__(
        self,
        base_url: str,
        api_key: str,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.max_retries = max_retries
        self.base_delay = base_delay

    def get_status(self, device_id: str) -> DeviceStatus:
        """Fetch the current status of a device.

        Retries automatically on 429 and 503 responses using exponential
        backoff. All other non-200 responses raise immediately.

        Args:
            device_id: The device identifier to look up.

        Returns:
            A ``DeviceStatus`` dataclass populated from the API response.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors, or when all
                retry attempts are exhausted.
        """
        url = f"{self.base_url}/devices/{device_id}"
        headers = {"X-API-Key": self.api_key}

        for attempt in range(self.max_retries + 1):
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                return DeviceStatus(
                    device_id=data["device_id"],
                    status=data["status"],
                    last_seen=data["last_seen"],
                    firmware_version=data["firmware_version"],
                )

            if response.status_code not in self._RETRYABLE_STATUSES or attempt == self.max_retries:
                response.raise_for_status()

            time.sleep(self.base_delay * (2 ** attempt))

        raise RuntimeError(f"Unreachable: retry loop exited without returning for {device_id}")


def fetch_all_statuses(
    device_ids: list[str],
    client: DeviceStatusClient,
) -> dict[str, DeviceStatus | str]:
    """Fetch statuses for multiple devices, capturing errors per device.

    Args:
        device_ids: List of device identifiers to query.
        client: A configured ``DeviceStatusClient`` instance.

    Returns:
        Dict mapping each ``device_id`` to either a ``DeviceStatus`` on
        success or an error message string on failure.
    """
    results: dict[str, DeviceStatus | str] = {}
    for device_id in device_ids:
        try:
            results[device_id] = client.get_status(device_id)
        except Exception as exc:
            results[device_id] = str(exc)
    return results
