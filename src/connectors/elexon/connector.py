# src/connectors/elexon/connector.py

import os
import logging
from datetime import date
from typing import Any, Dict
from dotenv import load_dotenv

from ..base import BaseConnector, RawData
from .api_client import ElexonApiClient
from .parameter_builder import ElexonParameterBuilder

load_dotenv()
logger = logging.getLogger(__name__)


class ElexonConnector(BaseConnector):
    """Connector for Elexon public dataset and BMRS endpoints."""

    def __init__(self, config: dict):
        super().__init__("elexon", config)

        api_key_config = self.config.get('api_key', '')
        if isinstance(api_key_config, str) and api_key_config.startswith("${") and api_key_config.endswith("}"):
            env_var_name = api_key_config[2:-1]
            resolved_api_key = os.getenv(env_var_name)
        else:
            resolved_api_key = api_key_config

        base_url = self.config.get('base_url')
        self._client = ElexonApiClient(api_key=resolved_api_key, base_url=base_url)
        self._param_builder = ElexonParameterBuilder()

    def build_url_for_report(self, report_config: dict) -> str:
        return self._client._build_url(report_config.get('code'), report_config.get('version'))

    def build_params_for_report(self, report_config: dict, current_date: date | None = None, period: int | None = None, from_datetime: str | None = None, to_datetime: str | None = None) -> Dict[str, Any]:
        # Use the parameter builder for a single date
        # Backwards-compatible wrapper: accept a single date or a date range via current_date and period unused.
        # If a single current_date is provided, use it as both start and end. Otherwise expect a tuple in current_date.
        if current_date is None:
            raise ValueError("current_date / start_date is required")
        # Support passing a tuple (start, end)
        if isinstance(current_date, tuple) and len(current_date) == 2:
            start_date, end_date = current_date
        else:
            start_date = current_date
            end_date = current_date

    def build_full_request_for_report(self, report_config: dict, start_date: date | None = None, end_date: date | None = None, period: int | None = None, from_datetime: str | None = None, to_datetime: str | None = None) -> str:
        return self._param_builder.build_params_for_report(report_config, start_date=start_date, end_date=end_date)

    def build_full_request_for_report(self, report_config: dict, start_date: date | None = None, end_date: date | None = None) -> str:
        # Return the fully encoded URL for a given report and date range
        if start_date is None:
            raise ValueError("start_date is required")
        if end_date is None:
            end_date = start_date
        base_url = self.config.get('base_url') or self._client.base_url
        return self._param_builder.build_full_request_url(report_config, base_url=base_url, start_date=start_date, end_date=end_date)

    def extract(self, start_date: date, end_date: date) -> list[RawData]:
        logger.info(f"Starting Elexon extraction for {len(self.config.get('reports', []))} report types.")
        all_raw = []
        for report in self.config.get('reports', []):
            report_name = report['name']
            logger.info(f"--- Processing report: '{report_name}' ---")
            for params, filename in self._param_builder.build_params_generator(report, start_date, end_date):
                raw_payload = self._client.make_request(report.get('code'), report.get('version'), params)
                if raw_payload and 'The API key is invalid' not in raw_payload:
                    data_to_save = RawData(payload=raw_payload, source_name=self.name, filename=filename)
                    all_raw.append(data_to_save)
        logger.info(f"Elexon extraction complete. Found {len(all_raw)} total files to save.")
        return all_raw
