# src/connectors/entsoe/api_client.py

import requests
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EntsoeApiClient:
    """Low-level HTTP client for ENTSO-E API calls."""

    REQUEST_TIMEOUT = 30

    def __init__(self, api_key: str, base_url: str):
        if not api_key:
            raise ValueError("ENTSO-E API key is required.")
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        # attach security token to session params for convenience
        self.session.params = {'securityToken': api_key}
        logger.info("Initialized EntsoeApiClient", extra={"base_url": self.base_url})

    def make_request(self, params: Dict[str, Any]) -> str | None:
        """Make a GET request to the ENTSO-E API and return text or None on error."""
        logger.info(f"ENTSO-E request: Params: {params}, URL: {self.base_url}")
        try:
            response = self.session.get(self.base_url, params=params, timeout=self.REQUEST_TIMEOUT)
            response.raise_for_status()
            logger.debug("ENTSO-E request successful", extra={"status_code": response.status_code, "url": response.url})
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"ENTSO-E request failed. Error: {str(e)}")
            return None

