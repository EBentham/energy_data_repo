# src/transformers/parsers/elexon_parser.py

import pandas as pd
import xml.etree.ElementTree as ET
import logging
import json
import re
from typing import IO, Any

logger = logging.getLogger(__name__)


# Note: The Elexon BMRS API often nests data within a <responseBody><data> structure.
# The public dataset API returns JSON structures; try to parse JSON first and fall back to XML.


def _read_content(file_obj: IO[str]) -> str:
    try:
        file_obj.seek(0)
    except Exception:
        pass
    return file_obj.read()


def _extract_items_from_json(obj: Any):
    """Find the first list of dict-like items in a JSON object by common keys or by searching.

    This is intentionally permissive to handle slightly different JSON shapes from Elexon.
    """
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj
        for item in obj:
            res = _extract_items_from_json(item)
            if res:
                return res
        return []

    if isinstance(obj, dict):
        # common container keys used by various APIs
        for k in ('responseBody', 'data', 'results', 'items', 'dataset', 'dataSets', 'rows', 'timeSeries', 'results'):
            if k in obj:
                return _extract_items_from_json(obj[k])
        # look for keys that map to lists of dicts
        for v in obj.values():
            res = _extract_items_from_json(v)
            if res:
                return res
    return []


# --- Parsers (JSON-first, XML fallback) ---

def parse_registered_capacity(xml_file: IO[str]) -> pd.DataFrame:
    """Parses a Registered Capacity (B1430) JSON or XML file into a pandas DataFrame."""
    content = _read_content(xml_file)

    # Try JSON first
    try:
        data = json.loads(content)
        items = _extract_items_from_json(data)
        records = []
        for item in items:
            records.append({
                'bm_unit_id': item.get('bmUnitID') or item.get('bm_unit_id') or item.get('bmUnitId'),
                'eic_code': item.get('eicCode') or item.get('eic_code'),
                'registered_capacity_mw': pd.to_numeric(item.get('registeredCapacity') or item.get('registered_capacity') or item.get('registeredCapacityMW'), errors='coerce'),
                'fuel_type': item.get('powerSystemResourceType') or item.get('power_system_resource_type') or item.get('fuelType'),
            })
        return pd.DataFrame(records)
    except Exception:
        pass

    # Fallback to XML
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.error(f"Parsing failed for registered capacity: {e}")
        return pd.DataFrame()

    records = []
    for item in root.findall('.//item'):
        records.append({
            'bm_unit_id': item.findtext('bmUnitID'),
            'eic_code': item.findtext('eicCode'),
            'registered_capacity_mw': pd.to_numeric(item.findtext('registeredCapacity')),
            'fuel_type': item.findtext('powerSystemResourceType'),
        })
    return pd.DataFrame(records)


def parse_generation_outages(xml_file: IO[str]) -> pd.DataFrame:
    """Parses a Generation Outages (B1510) JSON or XML file into a pandas DataFrame."""
    content = _read_content(xml_file)

    try:
        data = json.loads(content)
        items = _extract_items_from_json(data)
        records = []
        for item in items:
            records.append({
                'bm_unit_id': item.get('bMUnitID') or item.get('bmUnitID') or item.get('bm_unit_id'),
                'outage_start_utc': pd.to_datetime(item.get('startDateTimeUTC') or item.get('start_date_time_utc'), errors='coerce'),
                'outage_end_utc': pd.to_datetime(item.get('endDateTimeUTC') or item.get('end_date_time_utc'), errors='coerce'),
                'unavailable_capacity_mw': pd.to_numeric(item.get('capacityUnavailable') or item.get('capacity_unavailable'), errors='coerce'),
                'outage_type': item.get('outageType') or item.get('outage_type'),
            })
        return pd.DataFrame(records)
    except Exception:
        pass

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.error(f"Parsing failed for generation outages: {e}")
        return pd.DataFrame()

    records = []
    for item in root.findall('.//item'):
        records.append({
            'bm_unit_id': item.findtext('bMUnitID'),
            'outage_start_utc': pd.to_datetime(item.findtext('startDateTimeUTC')),
            'outage_end_utc': pd.to_datetime(item.findtext('endDateTimeUTC')),
            'unavailable_capacity_mw': pd.to_numeric(item.findtext('capacityUnavailable')),
            'outage_type': item.findtext('outageType'),
        })
    return pd.DataFrame(records)


def parse_physical_notifications(xml_file: IO[str]) -> pd.DataFrame:
    """Parses Physical Notifications (B0710) JSON or XML files into a pandas DataFrame."""
    content = _read_content(xml_file)

    try:
        data = json.loads(content)
        # look for timeSeries or similar structures
        ts_list = []
        if isinstance(data, dict) and 'timeSeries' in data:
            ts_list = data['timeSeries']
        else:
            ts_list = _extract_items_from_json(data)

        records = []
        for ts_item in ts_list:
            bm_unit_id = None
            if isinstance(ts_item, dict):
                bm_unit_id = ts_item.get('bMUnitID') or ts_item.get('bmUnitID') or ts_item.get('bm_unit_id')
                periods = ts_item.get('period', ts_item.get('periods') or ts_item.get('periodsList') or [])
                if isinstance(periods, dict):
                    periods = [periods]
                for period in periods:
                    start_time = pd.to_datetime(period.get('start') or period.get('startDateTime') or period.get('startDate'), errors='coerce')
                    resolution_str = period.get('resolution', 'PT30M')
                    # extract the first integer number from the resolution string (e.g. 'PT30M' -> 30)
                    m = re.search(r"(\d+)", resolution_str or "")
                    resolution_minutes = int(m.group(1)) if m else 30
                    points = period.get('point', period.get('points') or [])
                    if isinstance(points, dict):
                        points = [points]
                    for point in points:
                        position = int(point.get('position')) if point.get('position') is not None else None
                        quantity = pd.to_numeric(point.get('quantity') or point.get('value'), errors='coerce')
                        if position and start_time is not pd.NaT:
                            timestamp = start_time + pd.to_timedelta((position - 1) * resolution_minutes, unit='m')
                        else:
                            timestamp = None
                        records.append({
                            'timestamp_utc': timestamp,
                            'bm_unit_id': bm_unit_id,
                            'notification_mw': quantity,
                        })
        return pd.DataFrame(records)
    except Exception:
        pass

    # fallback to XML parsing
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.error(f"Parsing failed for physical notifications: {e}")
        return pd.DataFrame()

    records = []
    for ts_item in root.findall('.//timeSeries'):
        bm_unit_id = ts_item.findtext('.//bMUnitID')
        for period in ts_item.findall('.//period'):
            start_time = pd.to_datetime(period.findtext('.//start'))
            resolution_str = period.findtext('.//resolution', default='PT30M')  # Default to 30M if not present
            m = re.search(r"(\d+)", resolution_str or "")
            resolution_minutes = int(m.group(1)) if m else 30
            for point in period.findall('.//point'):
                position = int(point.findtext('position'))
                quantity = pd.to_numeric(point.findtext('quantity'))
                timestamp = start_time + pd.to_timedelta((position - 1) * resolution_minutes, unit='m')
                records.append({
                    'timestamp_utc': timestamp,
                    'bm_unit_id': bm_unit_id,
                    'notification_mw': quantity,
                })
    return pd.DataFrame(records)


def parse_bid_offer_data(xml_file: IO[str]) -> pd.DataFrame:
    """Parses Bid-Offer Level (BOALF) JSON or XML files into a pandas DataFrame."""
    content = _read_content(xml_file)

    try:
        data = json.loads(content)
        items = _extract_items_from_json(data)
        records = []
        for item in items:
            settlement_date = pd.to_datetime(item.get('settlementDate') or item.get('settlement_date'), errors='coerce')
            settlement_period = int(item.get('settlementPeriod') or item.get('settlement_period') or 0)
            timestamp = settlement_date + pd.to_timedelta((settlement_period - 0.5) * 30, unit='m')
            records.append({
                'timestamp_utc': timestamp,
                'bm_unit_id': item.get('bmUnitID') or item.get('bm_unit_id'),
                'bid_price_gbp_per_mwh': pd.to_numeric(item.get('bidPrice') or item.get('bid_price'), errors='coerce'),
                'offer_price_gbp_per_mwh': pd.to_numeric(item.get('offerPrice') or item.get('offer_price'), errors='coerce'),
                'bid_volume_mw': pd.to_numeric(item.get('bidVolume') or item.get('bid_volume'), errors='coerce'),
                'offer_volume_mw': pd.to_numeric(item.get('offerVolume') or item.get('offer_volume'), errors='coerce'),
            })
        return pd.DataFrame(records)
    except Exception:
        pass

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for bid-offer data: {e}")
        return pd.DataFrame()

    records = []
    for item in root.findall('.//item'):
        settlement_date = pd.to_datetime(item.findtext('settlementDate'))
        settlement_period = int(item.findtext('settlementPeriod'))
        timestamp = settlement_date + pd.to_timedelta((settlement_period - 0.5) * 30, unit='m')

        records.append({
            'timestamp_utc': timestamp,
            'bm_unit_id': item.findtext('bmUnitID'),
            'bid_price_gbp_per_mwh': pd.to_numeric(item.findtext('bidPrice')),
            'offer_price_gbp_per_mwh': pd.to_numeric(item.findtext('offerPrice')),
            'bid_volume_mw': pd.to_numeric(item.findtext('bidVolume')),
            'offer_volume_mw': pd.to_numeric(item.findtext('offerVolume')),
        })
    return pd.DataFrame(records)