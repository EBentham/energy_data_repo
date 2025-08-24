import pandas as pd
import xml.etree.ElementTree as ET
import logging
from typing import IO

logger = logging.getLogger(__name__)

def _get_namespace(root: ET.Element) -> dict:
    """
    Extracts the XML namespace from the root element of an ENTSO-E document.
    This is a crucial helper function because all find() operations need it.
    """
    # The tag name is formatted like '{urn:iec62325.351:etc}TagName'
    # This safely extracts the part within the curly braces.
    namespace = root.tag.split('}')[0].strip('{')
    return {'e': namespace}

def parse_generation(xml_file: IO[str]) -> pd.DataFrame:
    """
    Parses a Generation (A75) XML file from a file-like object into a pandas DataFrame.

    The function is designed to handle the specific structure of the ENTSO-E
    generation document, which contains multiple TimeSeries, one for each fuel type.

    Args:
        xml_file (IO[str]): An open file object containing the raw XML content.

    Returns:
        pd.DataFrame: A DataFrame with columns ['timestamp_utc', 'fuel_type', 'generation_mw'].
                      Returns an empty DataFrame if parsing fails or there's no data.
    """
    try:
        # 1. Parse the XML file into an ElementTree object and get the root
        root = ET.parse(xml_file).getroot()
        # 2. Extract the namespace to use in all subsequent searches
        ns = _get_namespace(root)
    except ET.ParseError as e:
        logger.error(f"XML parsing failed: {e}")
        return pd.DataFrame()

    records = []
    # 3. Find all 'TimeSeries' blocks. Each block represents one fuel type.
    for time_series in root.findall('e:TimeSeries', ns):
        # 4. Within each TimeSeries, find the fuel type code (e.g., B16 for Solar, B19 for Wind).
        psr_type_element = time_series.find('e:MktPSRType/e:psrType', ns)
        if psr_type_element is None: continue
        fuel_type = psr_type_element.text

        # 5. Find the 'Period' block, which contains the actual time-series data.
        period = time_series.find('e:Period', ns)
        if period is None: continue

        # 6. Extract metadata for the period: start time and resolution.
        start_time = pd.to_datetime(period.find('e:timeInterval/e:start', ns).text)
        resolution_str = period.find('e:resolution', ns).text
        # Convert resolution string like 'PT15M' (Period Time 15 Minutes) to an integer.
        resolution_minutes = int(resolution_str.strip('PTM'))

        # 7. Iterate through each 'Point' in the Period. Each point is a single measurement.
        for point in period.findall('e:Point', ns):
            # The position (1, 2, 3...) indicates the interval step.
            position = int(point.find('e:position', ns).text)
            # The generation quantity for that interval.
            quantity = float(point.find('e:quantity', ns).text)

            # 8. CRITICAL STEP: Calculate the actual timestamp for this data point.
            # The timestamp is the period's start time plus the number of intervals elapsed.
            timestamp = start_time + pd.to_timedelta((position - 1) * resolution_minutes, unit='m')

            # 9. Append the structured data record to our list.
            records.append({
                "timestamp_utc": timestamp,
                "fuel_type": fuel_type,       # e.g., 'B16'
                "generation_mw": quantity,    # e.g., 1500.5
            })

    # 10. Create the final pandas DataFrame from the list of records.
    return pd.DataFrame(records)


# --- NEW FUNCTION FOR PRICES ---
def parse_prices(xml_file: IO[str]) -> pd.DataFrame:
    """
    Parses a Day-Ahead Prices (A44) XML file into a pandas DataFrame.

    The structure is very similar to other documents, but the value we are
    interested in is 'price.amount' instead of 'quantity'.

    Args:
        xml_file (IO[str]): An open file object containing the raw XML content.

    Returns:
        pd.DataFrame: A DataFrame with columns ['timestamp_utc', 'price_eur_per_mwh'].
    """
    try:
        root = ET.parse(xml_file).getroot()
        ns = _get_namespace(root)
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for price data: {e}")
        return pd.DataFrame()

    records = []
    # Price documents usually contain a single TimeSeries block
    for time_series in root.findall('e:TimeSeries', ns):
        period = time_series.find('e:Period', ns)
        if period is None: continue

        # 1. Extract the period's start time and resolution (often PT60M for prices)
        start_time = pd.to_datetime(period.find('e:timeInterval/e:start', ns).text)
        resolution_str = period.find('e:resolution', ns).text
        # Handle different resolutions, e.g., PT60M, PT30M, PT15M
        if 'H' in resolution_str:  # e.g. PT1H
            resolution_minutes = int(resolution_str.strip('PTH')) * 60
        else:  # e.g. PT15M
            resolution_minutes = int(resolution_str.strip('PTM'))

        # 2. Iterate through each Point, which represents one time interval (e.g., one hour)
        for point in period.findall('e:Point', ns):
            position = int(point.find('e:position', ns).text)
            # 3. CRITICAL: The price value is in a 'price.amount' tag.
            price_element = point.find('e:price.amount', ns)
            if price_element is None: continue
            price = float(price_element.text)

            # 4. Calculate the timestamp for this price point
            timestamp = start_time + pd.to_timedelta((position - 1) * resolution_minutes, unit='m')

            records.append({
                "timestamp_utc": timestamp,
                "price_eur_per_mwh": price,
            })

    return pd.DataFrame(records)


# --- NEW FUNCTION FOR LOAD ---
def parse_load(xml_file: IO[str]) -> pd.DataFrame:
    """
    Parses a Total Load (A65) XML file into a pandas DataFrame.

    This function is very similar to parsing generation, but simpler as there's
    only one type of quantity (total load) and typically one TimeSeries.

    Args:
        xml_file (IO[str]): An open file object containing the raw XML content.

    Returns:
        pd.DataFrame: A DataFrame with columns ['timestamp_utc', 'load_mw'].
    """
    try:
        root = ET.parse(xml_file).getroot()
        ns = _get_namespace(root)
    except ET.ParseError as e:
        logger.error(f"XML parsing failed for load data: {e}")
        return pd.DataFrame()

    records = []
    # Load documents typically have only one TimeSeries
    for time_series in root.findall('e:TimeSeries', ns):
        period = time_series.find('e:Period', ns)
        if period is None: continue

        # 1. Extract the period's start time and resolution (often PT15M or PT30M)
        start_time = pd.to_datetime(period.find('e:timeInterval/e:start', ns).text)
        resolution_str = period.find('e:resolution', ns).text
        resolution_minutes = int(resolution_str.strip('PTM'))

        # 2. Iterate through each Point, representing one time interval
        for point in period.findall('e:Point', ns):
            position = int(point.find('e:position', ns).text)
            # 3. The load value is in a 'quantity' tag.
            quantity_element = point.find('e:quantity', ns)
            if quantity_element is None: continue
            quantity = float(quantity_element.text)

            # 4. Calculate the timestamp for this load point
            timestamp = start_time + pd.to_timedelta((position - 1) * resolution_minutes, unit='m')

            records.append({
                "timestamp_utc": timestamp,
                "load_mw": quantity,
            })

    return pd.DataFrame(records)


# You can add the cross-border flow parser here as well, as it's often similar to load
def parse_cross_border_flows(xml_file: IO[str]) -> pd.DataFrame:
    """Parses a Physical Flows (A81) XML file into a pandas DataFrame."""
    # The structure is identical to Total Load, we just rename the column for clarity
    df = parse_load(xml_file)
    return df.rename(columns={"load_mw": "flow_mw"})