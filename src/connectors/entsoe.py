# src/connectors/entsoe.py

import os
import requests
import logging
from datetime import date, timedelta
from typing import Any
from dotenv import load_dotenv

from .base import BaseConnector, RawData

# Load environment variables from .env file into the script's environment
load_dotenv()

# Set up a module-level logger
logger = logging.getLogger(__name__)


class _EntsoeApiClient:
    """
    Private helper class to handle all communication with the ENTSO-E API.
    This encapsulates the requests logic, keeping the main connector clean.
    """

    def __init__(self, api_key: str, base_url: str):
        if not api_key:
            raise ValueError("ENTSO-E API key is required.")
        self.base_url = base_url
        self.session = requests.Session()
        # The security token is added to all requests made with this session
        self.session.params = {'securityToken': api_key}

    def _make_request(self, params: dict[str, Any]) -> str | None:
        """
        Makes a single, generic request to the ENTSO-E API.

        Args:
            params (dict): The full set of query parameters for the request.

        Returns:
            The raw XML response text as a string, or None if the request fails.
        """
        logger.info(f"Querying ENTSO-E API with params: {params}")
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            logger.debug("Successfully fetched data from ENTSO-E API.")
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch ENTSO-E data: {e}")
            return None


class EntsoeConnector(BaseConnector):
    """
    A scalable connector for the ENTSO-E data source that uses a declarative
    configuration to build and execute API queries.
    """

    def __init__(self, config: dict):
        """
        Initializes the ENTSO-E connector and resolves the API key from the environment.
        """
        super().__init__("entsoe", config)

        # Resolve the API key: check for a placeholder and load from environment variables
        api_key_config = self.config.get('api_key', '')
        if api_key_config.startswith("${") and api_key_config.endswith("}"):
            env_var_name = api_key_config.strip("${}")
            resolved_api_key = os.getenv(env_var_name)
        else:
            resolved_api_key = api_key_config

        # Instantiate the API client with the resolved key and configured URL
        self._client = _EntsoeApiClient(
            api_key=resolved_api_key,
            base_url=self.config.get('base_url')
        )

    def extract(self, start_date: date, end_date: date) -> list[RawData]:
        """
        Extracts all configured data types from ENTSO-E for the given date range.

        This method iterates through the list of queries defined in config.yaml,
        builds the necessary parameters for each, and fetches the data day-by-day.
        """
        logger.info(f"Starting ENTSO-E extraction for {len(self.config.get('queries', []))} query types.")
        all_raw_data = []

        for query_config in self.config.get('queries', []):
            query_name = query_config['name']
            logger.info(f"--- Processing query: '{query_name}' ---")

            current_date = start_date
            while current_date <= end_date:
                # Build the complete parameter dictionary for this specific query and day
                params = self._build_params_for_day(query_config, current_date)

                # Make the API call
                raw_xml = self._client._make_request(params)

                if raw_xml:
                    # The filename now includes the query name to create subdirectories
                    # e.g., "generation_per_type/2024-01-15.xml"
                    filename = f"{query_name}/{current_date.strftime('%Y-%m-%d')}.xml"
                    data_to_save = RawData(
                        payload=raw_xml,
                        source_name=self.name,
                        filename=filename
                    )
                    all_raw_data.append(data_to_save)
                else:
                    logger.warning(f"No data returned for query '{query_name}' on {current_date}.")

                # Move to the next day
                current_date += timedelta(days=1)

        logger.info(f"ENTSO-E extraction complete. Found {len(all_raw_data)} total files to save.")
        return all_raw_data

    def _resolve_placeholder(self, placeholder: str) -> str:
        """
        Resolves placeholder values (e.g., "${primary_bidding_zone}") from the
        connector's configuration dictionary. Handles simple and nested keys.
        """
        # Remove the placeholder syntax, e.g., "${interconnectors.FR}" -> "interconnectors.FR"
        clean_placeholder = placeholder.strip("${}")

        # Handle nested placeholders by splitting the key string
        keys = clean_placeholder.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            logger.error(f"Could not resolve placeholder: '{placeholder}' in the ENTSO-E config.")
            return ""

    def _build_params_for_day(self, query_config: dict, query_date: date) -> dict[str, Any]:
        """
        A helper method to construct the final parameter dictionary for an API call
        based on a declarative query configuration.
        """
        # 1. Start with static parameters defined for the query in the config
        params = query_config.get('params', {}).copy()

        # 2. Add the mandatory document type
        params['documentType'] = query_config['documentType']

        # 3. Add the dynamic date parameters in the required format
        params['periodStart'] = query_date.strftime('%Y%m%d%H%M')
        params['periodEnd'] = (query_date + timedelta(days=1)).strftime('%Y%m%d%H%M')

        # 4. Explicitly build domain parameters by resolving placeholders from the config
        domain_params = query_config.get('domain_params', {})
        for param_name, placeholder in domain_params.items():
            # For each domain param (e.g., 'in_Domain', 'outBiddingZone_Domain'),
            # resolve its placeholder value from the config.
            resolved_value = self._resolve_placeholder(placeholder)
            if resolved_value:
                params[param_name] = resolved_value

        return params