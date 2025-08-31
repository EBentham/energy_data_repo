# src/transformers/parsers/elexon_parser.py

import pandas as pd
import xml.etree.ElementTree as ET
import logging
from typing import IO

logger = logging.getLogger(__name__)


# Note: The Elexon BMRS API often nests data within a <responseBody><data> structure.
# And the main data is often in a list of <item> tags.

def parse_registered_capacity(xml_file: IO[str]) -> pd.DataFrame:
    """Parses a Registered Capacity (B1430) XML file into a pandas DataFrame."""
    try:
        root = ET.parse(xml_file).getroot()
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for registered capacity: {e}")
        return pd.DataFrame()

    records = []
    # Find all <item> tags within the response data
    for item in root.findall('.//item'):
        records.append({
            'bm_unit_id': item.findtext('bmUnitID'),
            'eic_code': item.findtext('eicCode'),
            'registered_capacity_mw': pd.to_numeric(item.findtext('registeredCapacity')),
            'fuel_type': item.findtext('powerSystemResourceType'),
        })
    return pd.DataFrame(records)


def parse_generation_outages(xml_file: IO[str]) -> pd.DataFrame:
    """Parses a Generation Outages (B1510) XML file into a pandas DataFrame."""
    try:
        root = ET.parse(xml_file).getroot()
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for generation outages: {e}")
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
    """Parses Physical Notifications (B0710) XML files into a pandas DataFrame."""
    try:
        root = ET.parse(xml_file).getroot()
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for physical notifications: {e}")
        return pd.DataFrame()

    records = []
    # The structure here can be more complex, often with a header and a list of time series
    for ts_item in root.findall('.//timeSeries'):
        bm_unit_id = ts_item.findtext('.//bMUnitID')

        # Inside each time series is a period with points
        for period in ts_item.findall('.//period'):
            start_time = pd.to_datetime(period.findtext('.//start'))
            resolution_str = period.findtext('.//resolution', default='PT30M')  # Default to 30M if not present
            resolution_minutes = int(resolution_str.strip('PTM'))

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
    """Parses Bid-Offer Level (BOALF) XML files into a pandas DataFrame."""
    try:
        root = ET.parse(xml_file).getroot()
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for bid-offer data: {e}")
        return pd.DataFrame()

    records = []
    for item in root.findall('.//item'):
        # Extract the settlement date and period from a parent tag if available,
        # or fall back to values within the item.
        settlement_date = pd.to_datetime(item.findtext('settlementDate'))
        settlement_period = int(item.findtext('settlementPeriod'))

        # Calculate the timestamp for this half-hour period
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