# src/connectors/elexon.py

import os
import requests
import logging
from datetime import date, timedelta
from typing import Any, Dict, Generator, Tuple
from dotenv import load_dotenv

from .base import BaseConnector, RawData

load_dotenv()
logger = logging.getLogger(__name__)


class _ElexonApiClient:
    """Private helper class to handle all communication with the Elexon BMRS API."""

    def __init__(self, api_key: str, base_url: str):
        if not api_key and 'data.elexon.co.uk' not in (base_url or ""):
            # only require API key when using BMRS api.bmreports endpoints
            raise ValueError("Elexon API key is required for BMRS API endpoints.")
        self.base_url = base_url
        self.api_key = api_key  # Store the key directly
        self.session = requests.Session()

        # mapping for public data.elexon endpoints -> path
        self._public_code_path_map = {
            'B1620': 'datasets/AGPT',
            'B1610': 'demand/actual/total',
        }

    def _make_request(self, report_code: str, version: str, params: dict[str, Any]) -> str | None:
        """Makes a single, generic request to the BMRS API or the public Elexon data API.

        This method detects whether the configured base_url points to data.elexon.co.uk
        and adapts the path and parameters accordingly.
        """
        # Copy params to avoid mutation
        final_params = params.copy()

        is_public = self.base_url and 'data.elexon.co.uk' in self.base_url

        if is_public:
            # Public Elexon data endpoints don't use APIKey/ServiceType; they expect format=json
            # Map report codes to public paths where needed
            path = self._public_code_path_map.get(report_code, f"{report_code}/{version}")
            url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
            # ensure format is present
            if 'format' not in final_params:
                final_params['format'] = 'json'
        else:
            # BMRS endpoints (api.bmreports.com) expect APIKey and ServiceType
            final_params['APIKey'] = self.api_key
            final_params['ServiceType'] = 'xml'
            url = f"{self.base_url.rstrip('/')}/{report_code}/{version}"

        logger.info(f"Querying Elexon API at '{url}' with params: {final_params}")
        response = None
        try:
            response = self.session.get(url, params=final_params, timeout=30)
            response.raise_for_status()
            logger.debug("Successfully fetched data from Elexon API.")
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to fetch Elexon BMRS data: {e}. URL: {response.url if response is not None else url}")
            return None


class ElexonConnector(BaseConnector):
    """A scalable connector for the Elexon BMRS data source."""

    def __init__(self, config: dict):
        """Initializes the Elexon connector and resolves the API key."""
        super().__init__("elexon", config)

        api_key_config = self.config.get('api_key', '')
        if api_key_config.startswith("${") and api_key_config.endswith("}"):
            env_var_name = api_key_config.strip("${}")
            resolved_api_key = os.getenv(env_var_name)
        else:
            resolved_api_key = api_key_config

        # allow running against the public data.elexon service (no API key required)
        self._client = _ElexonApiClient(
            api_key=resolved_api_key,
            base_url=self.config.get('base_url')
        )

    # New helper: build url for a report (no network)
    def build_url_for_report(self, report_config: dict) -> str:
        code = report_config.get('code')
        version = report_config.get('version')
        base = self.config.get('base_url', '').rstrip('/')
        # if using public data.elexon base, map codes to their public path
        if base and 'data.elexon.co.uk' in base:
            public_map = self._client._public_code_path_map
            path = public_map.get(code, f"{code}/{version}")
            return f"{base}/{path}"
        return f"{base}/{code}/{version}"

    # New helper: build params dict for a single report call given a date and optional period
    def build_params_for_report(self, report_config: dict, current_date: date | None = None, period: int | None = None) -> Dict[str, Any]:
        params = report_config.get('params', {}).copy()

        base = self.config.get('base_url', '')
        is_public = base and 'data.elexon.co.uk' in base

        # For public endpoints we don't attach APIKey or ServiceType, instead use 'format'
        if not is_public:
            params['APIKey'] = self._client.api_key
            params['ServiceType'] = 'xml'

        date_type = report_config.get('date_param_type')
        if date_type is None:
            # no time-series params
            return params

        if date_type == 'from_to_date' and current_date:
            if is_public and report_config.get('code') == 'B1620':
                # Public AGPT endpoint expects publishDateTimeFrom/To with time
                # Use midnight if time not supplied
                params['publishDateTimeFrom'] = f"{current_date.strftime('%Y-%m-%d')} 00:00"
                params['publishDateTimeTo'] = f"{current_date.strftime('%Y-%m-%d')} 23:59"
            else:
                params['FromDate'] = current_date.strftime('%Y-%m-%d')
                params['ToDate'] = current_date.strftime('%Y-%m-%d')
            return params

        if date_type == 'settlement_date_period' and current_date:
            if is_public and report_config.get('code') == 'B1610':
                # Public actual total load endpoint expects from/to and settlementPeriodFrom/To
                params['from'] = current_date.strftime('%Y-%m-%d')
                params['to'] = current_date.strftime('%Y-%m-%d')
                if period is not None:
                    params['settlementPeriodFrom'] = str(period)
                    params['settlementPeriodTo'] = str(period)
            else:
                params['SettlementDate'] = current_date.strftime('%Y-%m-%d')
                if period is not None:
                    params['Period'] = str(period)
            return params

        return params

    def extract(self, start_date: date, end_date: date) -> list[RawData]:
        """Extracts all configured report data from Elexon BMRS for the given date range."""
        logger.info(f"Starting Elexon BMRS extraction for {len(self.config.get('reports', []))} report types.")
        all_raw_data = []

        for report_config in self.config.get('reports', []):
            report_name = report_config['name']
            logger.info(f"--- Processing report: '{report_name}' ---")

            param_generator = self._build_params_for_report(report_config, start_date, end_date)

            for params, filename in param_generator:
                raw_xml = self._client._make_request(
                    report_code=report_config['code'],
                    version=report_config['version'],
                    params=params
                )
                if raw_xml and 'The API key is invalid' not in raw_xml:  # Added check for common error message
                    data_to_save = RawData(
                        payload=raw_xml,
                        source_name=self.name,
                        filename=filename
                    )
                    all_raw_data.append(data_to_save)

        logger.info(f"Elexon BMRS extraction complete. Found {len(all_raw_data)} total files to save.")
        return all_raw_data

    def _build_params_for_report(self, report_config: dict, start_date: date, end_date: date) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        """
        A generator that constructs the parameters for each API call based on the
        report's date requirements.
        """
        report_name = report_config['name']
        date_type = report_config.get('date_param_type')
        base_params = report_config.get('params', {}).copy()

        base = self.config.get('base_url', '')
        is_public = base and 'data.elexon.co.uk' in base

        if date_type is None:
            logger.info("Extracting non-time-series report.")
            filename = f"{report_name}/{report_name}.xml"
            # Attach credentials/service to base_params for the actual call
            if not is_public:
                base_params['APIKey'] = self._client.api_key
                base_params['ServiceType'] = 'xml'
            else:
                base_params['format'] = 'json'
            yield base_params, filename
            return

        current_date = start_date
        while current_date <= end_date:
            if date_type == "from_to_date":
                params = base_params.copy()
                if is_public and report_config.get('code') == 'B1620':
                    params['publishDateTimeFrom'] = f"{current_date.strftime('%Y-%m-%d')} 00:00"
                    params['publishDateTimeTo'] = f"{current_date.strftime('%Y-%m-%d')} 23:59"
                else:
                    params['FromDate'] = current_date.strftime('%Y-%m-%d')
                    params['ToDate'] = current_date.strftime('%Y-%m-%d')
                if not is_public:
                    params['APIKey'] = self._client.api_key
                    params['ServiceType'] = 'xml'
                else:
                    params['format'] = 'json'
                filename = f"{report_name}/{current_date.strftime('%Y-%m-%d')}.xml"
                yield params, filename

            elif date_type == "settlement_date_period":
                if is_public and report_config.get('code') == 'B1610':
                    # Public endpoint expects a single call per day with settlementPeriodFrom/To range
                    params = base_params.copy()
                    params['from'] = current_date.strftime('%Y-%m-%d')
                    params['to'] = current_date.strftime('%Y-%m-%d')
                    # default to full-day range if no specific period handling required
                    params['settlementPeriodFrom'] = '1'
                    params['settlementPeriodTo'] = '48'
                    params['format'] = 'json'
                    filename = f"{report_name}/{current_date.strftime('%Y-%m-%d')}.json"
                    yield params, filename
                else:
                    for period in range(1, 49):
                        params = base_params.copy()
                        params['SettlementDate'] = current_date.strftime('%Y-%m-%d')
                        params['Period'] = str(period)
                        params['APIKey'] = self._client.api_key
                        params['ServiceType'] = 'xml'
                        filename = f"{report_name}/{current_date.strftime('%Y-%m-%d')}_P{period:02d}.xml"
                        yield params, filename

            current_date += timedelta(days=1)

