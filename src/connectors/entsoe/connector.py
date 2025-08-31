# src/connectors/entsoe/connector.py

import os
import logging
from datetime import date, timedelta
from typing import Any, Dict
from dotenv import load_dotenv

from ..base import BaseConnector, RawData
from .api_client import EntsoeApiClient
from .parameter_builder import EntsoeParameterBuilder

# Load environment variables from .env file into the script's environment
load_dotenv()

# Set up a module-level logger
logger = logging.getLogger(__name__)


class EntsoeConnector(BaseConnector):
    """
    A scalable connector for the ENTSO-E data source that uses a declarative
    configuration to build and execute API queries. Refactored to use a
    dedicated API client and parameter builder.
    """

    def __init__(self, config: dict):
        """Initializes the ENTSO-E connector and resolves the API key from the environment."""
        super().__init__("entsoe", config)

        # Resolve the API key: check for a placeholder and load from environment variables
        api_key_config = self.config.get('api_key', '')
        if isinstance(api_key_config, str) and api_key_config.startswith("${") and api_key_config.endswith("}"):
            env_var_name = api_key_config[2:-1]
            resolved_api_key = os.getenv(env_var_name)
        else:
            resolved_api_key = api_key_config

        # Initialize components
        self._client = EntsoeApiClient(api_key=resolved_api_key, base_url=self.config.get('base_url'))
        self._param_builder = EntsoeParameterBuilder()

    def extract(self, start_date: date, end_date: date) -> list[RawData]:
        """Extracts all configured data types from ENTSO-E for the given date range.

        Iterates queries and days, builds params via the parameter builder (with
        placeholders resolved), calls the API client, and returns RawData objects.
        """
        logger.info(f"Starting ENTSO-E extraction for {len(self.config.get('queries', []))} query types.")
        all_raw_data: list[RawData] = []

        for query_config in self.config.get('queries', []):
            query_name = query_config['name']
            logger.info(f"--- Processing query: '{query_name}' ---")

            # Resolve domain placeholders into a copy of the query_config
            qc = dict(query_config)
            domain_params = qc.get('domain_params', {}) or {}
            resolved_domain = {}
            for param_name, placeholder in domain_params.items():
                resolved_domain[param_name] = self._resolve_placeholder(placeholder)
            if resolved_domain:
                qc['domain_params'] = resolved_domain

            current_date = start_date
            while current_date <= end_date:
                # Build parameters for this day using the parameter builder
                params = self._param_builder.build_params_for_day(qc, current_date)

                # Make the API call via the client
                raw_text = self._client.make_request(params)

                if raw_text:
                    filename = f"{query_name}/{current_date.strftime('%Y-%m-%d')}.xml"
                    data_to_save = RawData(payload=raw_text, source_name=self.name, filename=filename)
                    all_raw_data.append(data_to_save)
                else:
                    logger.warning(f"No data returned for query '{query_name}' on {current_date}.")

                current_date += timedelta(days=1)

        logger.info(f"ENTSO-E extraction complete. Found {len(all_raw_data)} total files to save.")
        return all_raw_data

    def _resolve_placeholder(self, placeholder: str) -> str:
        """Resolve placeholders like ${primary_bidding_zone} from the connector config."""
        if not isinstance(placeholder, str):
            return ""

        if placeholder.startswith("${") and placeholder.endswith("}"):
            clean_placeholder = placeholder[2:-1]
        else:
            clean_placeholder = placeholder

        # Support nested keys separated by dots
        keys = clean_placeholder.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            logger.error(f"Could not resolve placeholder: '{placeholder}' in the ENTSO-E config.")
            return ""

    # Public helpers for tests and external use
    def build_url_for_query(self, query_config: dict) -> str:
        """Return the base URL to be used for this query (public helper for tests)."""
        return self._client.base_url or self.config.get('base_url')

    def build_params_for_query(self, query_config: dict, query_date: date) -> Dict[str, Any]:
        """Return the fully expanded params for a given query and date (public helper for tests)."""
        # Resolve domain placeholders for this single query
        qc = dict(query_config)
        domain_params = qc.get('domain_params', {}) or {}
        resolved_domain = {}
        for param_name, placeholder in domain_params.items():
            resolved_domain[param_name] = self._resolve_placeholder(placeholder)
        if resolved_domain:
            qc['domain_params'] = resolved_domain

        params = self._param_builder.build_params_for_day(qc, query_date)
        # Ensure security token is present in params for testing convenience
        params['securityToken'] = self._client.api_key
        return params

