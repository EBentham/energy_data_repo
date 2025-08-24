# src/transformers/staging/stg_entsoe_generation.py

import pandas as pd
import logging
from pathlib import Path
from typing import List

# Import the specific parser function
from src.transformers.parsers.entsoe_parser import parse_generation
# --- NEW: Import the mapping dictionary ---
from src.transformers.mappings import ENTSOE_GENERATION_TYPE_MAP

logger = logging.getLogger(__name__)


def create_entsoe_generation_silver(source_bronze_path_str: str, source_silver_path_str: str) -> bool:
    """
    Transforms raw Bronze ENTSO-E generation data into a clean Silver table.

    Args:
        source_bronze_path_str (str): Path to the source's bronze data (e.g., 'data/bronze/entsoe').
        source_silver_path_str (str): Path to the source's silver data (e.g., 'data/silver/entsoe').
    """
    query_name = "generation_per_type"
    # The paths are now constructed from the arguments passed by the orchestrator.
    bronze_query_path = Path(source_bronze_path_str) / query_name
    silver_query_path = Path(source_silver_path_str) / query_name

    logger.info(f"Starting transformation for '{query_name}'.")

    # --- Steps 1 & 2: Find and parse all files (same as before) ---
    xml_files = list(bronze_query_path.glob("**/*.xml"))
    if not xml_files:
        logger.warning(f"No raw files found for '{query_name}'. Skipping.")
        return True

    logger.info(f"Found {len(xml_files)} files to process.")
    all_dfs: List[pd.DataFrame] = []
    for xml_file in xml_files:
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                df = parse_generation(f)
                if not df.empty:
                    all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse file '{xml_file}': {e}")

    if not all_dfs:
        logger.warning(f"No valid data produced after parsing for '{query_name}'.")
        return True

    # --- Step 3: Combine all daily DataFrames ---
    final_df = pd.concat(all_dfs, ignore_index=True)

    # --- Step 4: DATA ENRICHMENT & CLEANING ---
    # NEW: Use the map() function to create a new column with the human-readable names.
    # The .get(code, code) part is a safe way to handle it: if a code is not
    # found in our map, it will just use the original code instead of failing.
    final_df['fuel_type_name'] = final_df['fuel_type'].map(ENTSOE_GENERATION_TYPE_MAP)

    # Reorder columns for better readability. Put the new column next to the code.
    final_df = final_df[[
        "timestamp_utc",
        "fuel_type",
        "fuel_type_name",
        "generation_mw"
    ]]

    # Perform final sorting and de-duplication
    final_df = final_df.sort_values(by=["timestamp_utc", "fuel_type"]).drop_duplicates().reset_index(drop=True)

    # --- Step 5: Save the enriched DataFrame to the Silver layer ---
    try:
        silver_query_path.mkdir(parents=True, exist_ok=True)
        output_file = silver_query_path / "generation.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Successfully created enriched Silver table at '{output_file}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to save Silver table for '{query_name}': {e}")
        return False