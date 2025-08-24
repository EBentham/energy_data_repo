# src/transformers/staging/stg_entsoe_prices.py

import pandas as pd
import logging
from pathlib import Path
from typing import List

# Import the specific parser function we need
from src.transformers.parsers.entsoe_parser import parse_prices

logger = logging.getLogger(__name__)


def create_entsoe_prices_silver(source_bronze_path_str: str, source_silver_path_str: str) -> bool:
    """
    Transforms raw Bronze ENTSO-E day-ahead price data (A44) into a clean Silver table.
    """
    query_name = "day_ahead_prices"

    bronze_query_path = Path(source_bronze_path_str) / query_name
    silver_query_path = Path(source_silver_path_str) / query_name

    logger.info(f"Starting transformation for '{query_name}'.")

    # 1. Find all relevant raw files
    xml_files = list(bronze_query_path.glob("**/*.xml"))
    if not xml_files:
        logger.warning(f"No raw files found for '{query_name}'. Skipping.")
        return True

    logger.info(f"Found {len(xml_files)} files to process for '{query_name}'.")

    # 2. Parse all files and collect DataFrames
    all_dfs: List[pd.DataFrame] = []
    for xml_file in xml_files:
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                df = parse_prices(f)
                if not df.empty:
                    all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse file '{xml_file}': {e}")

    if not all_dfs:
        logger.warning(f"No valid data produced after parsing all files for '{query_name}'.")
        return True

    # 3. Combine, sort, and de-duplicate
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df = final_df.sort_values(by="timestamp_utc").drop_duplicates().reset_index(drop=True)

    # 4. Save the final DataFrame to the Silver layer
    try:
        silver_query_path.mkdir(parents=True, exist_ok=True)
        output_file = silver_query_path / "prices.csv"  # A single, consolidated output file
        final_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Successfully created Silver table at '{output_file}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to save Silver table for '{query_name}': {e}")
        return False