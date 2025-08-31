import requests
import logging
from typing import Any, Dict
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class ElexonApiClient:
    """Pure API communication client for Elexon BMRS and public data endpoints."""

    # Configuration constants
    SETTLEMENT_PERIODS = 48
    PUBLIC_ENDPOINTS = {
        'B1620': 'generation/actual/per-type',
        'B1610': 'demand/actual/total',
    }
    BMRS_SERVICE_TYPE = 'xml'
    PUBLIC_SERVICE_TYPE = 'json'
    REQUEST_TIMEOUT = 30

    def __init__(self, api_key: str, base_url: str):
        """
        Initialize the API client.

        Args:
            api_key: API key for BMRS endpoints (can be empty for public endpoints)
            base_url: Base URL for the API
        """
        self.api_key = api_key
        self.base_url = (base_url or '').rstrip('/')
        self.is_public = 'data.elexon.co.uk' in (base_url or '')
        self.session = requests.Session()

        logger.info("Initialized ElexonApiClient",
                   extra={"base_url": self.base_url, "is_public": self.is_public, "has_api_key": bool(api_key)})

    def make_request(self, report_code: str, version: str, params: Dict[str, Any]) -> str | None:
        """
        Make a request to the Elexon API.

        Args:
            report_code: The report code to request
            version: The API version
            params: Query parameters

        Returns:
            Response text or None if request failed
        """
        url = self._build_url(report_code, version)
        final_params = self._prepare_params(params.copy())

        logger.debug("Making API request", extra={
                    "url": url,
                    "report_code": report_code,
                    "version": version,
                    "is_public": self.is_public})

        try:
            response = self.session.get(
                url,
                params=final_params,
                timeout=self.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            logger.debug("API request successful", extra={
                        "status_code": response.status_code,
                        "url": response.url})

            return response.text

        except requests.exceptions.RequestException as e:
            logger.error("API request failed", extra={
                        "error": str(e),
                        "url": url,
                        "report_code": report_code,
                        "version": version})
            return None

    def _build_url(self, report_code: str, version: str) -> str:
        """
        Build the complete URL for the request.

        Args:
            report_code: The report code
            version: The API version

        Returns:
            Complete URL string
        """
        if self.is_public:
            # Use public endpoint mapping if available
            path = self.PUBLIC_ENDPOINTS.get(report_code, f"{report_code}/{version}")
            return urljoin(self.base_url + '/', path)
        else:
            # Standard BMRS endpoint
            return urljoin(self.base_url + '/', f"{report_code}/{version}")

    def _prepare_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare parameters for the API request.

        Args:
            params: Original parameters

        Returns:
            Prepared parameters with authentication and service type
        """
        if self.is_public:
            # Public endpoints use 'format' parameter
            if 'format' not in params:
                params['format'] = self.PUBLIC_SERVICE_TYPE
        else:
            # BMRS endpoints require API key and service type
            if not self.api_key:
                raise ValueError("API key is required for BMRS endpoints")
            params['APIKey'] = self.api_key
            params['ServiceType'] = self.BMRS_SERVICE_TYPE

        return params

    def validate_api_key(self) -> bool:
        """
        Validate the API key by making a test request.

        Returns:
            True if API key is valid, False otherwise
        """
        if self.is_public:
            logger.info("Skipping API key validation for public endpoint")
            return True

        if not self.api_key:
            logger.error("No API key provided for BMRS endpoint")
            return False

        # Make a simple test request
        test_params = {'APIKey': self.api_key, 'ServiceType': 'json'}
        response = self.session.get(
            f"{self.base_url}/B1610/v1",
            params=test_params,
            timeout=10
        )

        is_valid = response.status_code == 200 and 'The API key is invalid' not in response.text
        logger.info("API key validation result", extra={"is_valid": is_valid})

        return is_valid
