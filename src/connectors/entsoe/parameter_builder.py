# src/connectors/entsoe/parameter_builder.py

import logging
from datetime import date, timedelta
from typing import Any, Dict, Generator

logger = logging.getLogger(__name__)


class EntsoeParameterBuilder:
    """Builds parameters for ENTSO-E API queries."""

    def build_params_for_day(self, query_config: dict, query_date: date) -> Dict[str, Any]:
        """Return the parameter dict for one day for the given query_config."""
        params = query_config.get('params', {}).copy()

        # Mandatory documentType
        if 'documentType' in query_config:
            params['documentType'] = query_config['documentType']

        # periodStart and periodEnd are required for ENTSO-E
        params['periodStart'] = query_date.strftime('%Y%m%d%H%M')
        params['periodEnd'] = (query_date + timedelta(days=1)).strftime('%Y%m%d%H%M')

        # Resolve domain params placeholders if present
        domain_params = query_config.get('domain_params', {})
        for param_name, placeholder in domain_params.items():
            # Placeholder should be resolved by the caller (connector) against config; here we accept literal values
            params[param_name] = placeholder

        return params

    def build_params_generator(self, query_config: dict, start_date: date, end_date: date) -> Generator[Dict[str, Any], None, None]:
        """Yield parameters for each day in the date range."""
        current_date = start_date
        while current_date <= end_date:
            yield self.build_params_for_day(query_config, current_date)
            current_date += timedelta(days=1)

    def build_full_request_url(self, base_url: str, params: Dict[str, Any]) -> str:
        """Return a full encoded URL given base_url and params (for debugging)."""
        import urllib.parse
        query = urllib.parse.urlencode(params, doseq=True, quote_via=urllib.parse.quote)
        return f"{base_url}?{query}"

