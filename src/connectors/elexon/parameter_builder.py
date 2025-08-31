# src/connectors/elexon/parameter_builder.py

import logging
from datetime import date, timedelta
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ElexonParameterBuilder:
    """Minimal parameter builder for Elexon public endpoints as requested.

    - Uses fixed base paths for B1620 and B1610
    - Always sets settlementPeriodFrom=1 and settlementPeriodTo=36 unless overridden
    - Produces a single request covering start_date..end_date for these reports
    """

    PATH_MAP = {
        'B1620': 'generation/actual/per-type',
        'B1610': 'demand/actual/total',
    }

    DEFAULT_SP_FROM = 1
    DEFAULT_SP_TO = 36

    def __init__(self):
        logger.debug("Initialized simplified ElexonParameterBuilder")

    def build_params_for_report(self, report_config: Dict[str, Any], start_date: date, end_date: date) -> Dict[str, Any]:
        """Return params for a report covering the given date range.

        For B1610 and B1620 returns {'from': start, 'to': end, 'settlementPeriodFrom':1, 'settlementPeriodTo':36, 'format':'json'}
        For other reports, returns report_config.params unchanged.
        """
        code = report_config.get('code')
        params = report_config.get('params', {}).copy()

        if code in ('B1610', 'B1620'):
            params['from'] = start_date.strftime('%Y-%m-%d')
            params['to'] = end_date.strftime('%Y-%m-%d')
            # allow overrides in config
            params['settlementPeriodFrom'] = params.get('settlementPeriodFrom', self.DEFAULT_SP_FROM)
            params['settlementPeriodTo'] = params.get('settlementPeriodTo', self.DEFAULT_SP_TO)
            params['format'] = 'json'

        return params

    def build_params_generator(self, report_config: Dict[str, Any], start_date: date, end_date: date):
        """Yield a single (params, filename) tuple for the requested range for B1610/B1620.

        For other reports, yield daily FromDate/ToDate entries if date_param_type == 'from_to_date',
        or per-period entries if needed (kept minimal here).
        """
        code = report_config.get('code')
        report_name = report_config.get('name')

        if code in ('B1610', 'B1620'):
            params = self.build_params_for_report(report_config, start_date, end_date)
            filename = f"{report_name}/{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.json"
            yield params, filename
            return

        # Fallback minimal behaviour: daily from_to_date if requested
        date_type = report_config.get('date_param_type')
        if date_type == 'from_to_date':
            current = start_date
            while current <= end_date:
                params = {'FromDate': current.strftime('%Y-%m-%d'), 'ToDate': current.strftime('%Y-%m-%d')}
                filename = f"{report_name}/{current.strftime('%Y-%m-%d')}.xml"
                yield params, filename
                current += timedelta(days=1)
        else:
            # No time-series handling, return static params
            params = report_config.get('params', {}).copy()
            filename = f"{report_name}/{report_name}.json"
            yield params, filename

    def build_full_request_url(self, report_config: Dict[str, Any], base_url: str, start_date: date, end_date: date) -> str:
        """Build full encoded URL using base_url and params for the given range."""
        import urllib.parse

        code = report_config.get('code')
        path = self.PATH_MAP.get(code, f"{code}/{report_config.get('version')}")
        url = f"{base_url.rstrip('/')}/{path}"
        params = self.build_params_for_report(report_config, start_date, end_date)
        query = urllib.parse.urlencode(params, doseq=True, quote_via=urllib.parse.quote)
        return f"{url}?{query}"
