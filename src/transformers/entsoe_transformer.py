# src/transformers/entsoe_transformer.py

import logging
from pathlib import Path

# Import our custom parsers
from src.transformers.parsers import entsoe_parser

logger = logging.getLogger(__name__)

# --- PARSER MAPPING ---
# This dictionary maps the query name (which is also the directory name)
# to the correct parsing function. This makes the transformer easily extensible.
PARSER_MAP = {
    "generation_per_type": entsoe_parser.parse_generation,
    "day_ahead_prices": entsoe_parser.parse_prices,
    "total_load": entsoe_parser.parse_load,
    # "cross_border_flows_fr": entsoe_parser.parse_cross_border_flows,
    # "cross_border_flows_nl": entsoe_parser.parse_cross_border_flows,
    # Add other cross-border flows here if you have them
}


def transform_entsoe_data(bronze_path_str: str, silver_path_str: str):
    """
    Orchestrates the transformation of all ENTSO-E data from Bronze (XML)
    to Silver (CSV).

    Args:
        bronze_path_str (str): The path to the root of the Bronze layer.
        silver_path_str (str): The path to the root of the Silver layer.
    """
    bronze_path = Path(bronze_path_str) / "entsoe"
    silver_path = Path(silver_path_str) / "entsoe"

    logger.info(f"Starting ENTSO-E transformation from '{bronze_path}' to '{silver_path}'.")

    # Use glob to recursively find all XML files in the entsoe bronze directory
    xml_files = list(bronze_path.glob("**/*.xml"))
    if not xml_files:
        logger.warning(f"No XML files found in '{bronze_path}'. Nothing to transform.")
        return

    logger.info(f"Found {len(xml_files)} XML files to process.")

    for xml_file in xml_files:
        # The query name is the name of the parent directory
        # e.g., 'generation_per_type' from '.../generation_per_type/2024-01-15.xml'
        query_name = xml_file.parent.name

        # Look up the correct parser function from our map
        parser_func = PARSER_MAP.get(query_name)

        if not parser_func:
            logger.warning(f"No parser available for query type '{query_name}'. Skipping file: {xml_file}")
            continue

        logger.debug(f"Processing '{xml_file}' with parser '{parser_func.__name__}'.")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                # Call the parser function to get a DataFrame
                df = parser_func(f)

            if df.empty:
                logger.warning(f"Parsing resulted in an empty DataFrame for file: {xml_file}")
                continue

            # --- Save the transformed data to the Silver layer ---
            # The output path should mirror the input path structure.
            output_dir = silver_path / query_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Change the file extension from .xml to .csv
            output_file_path = output_dir / xml_file.with_suffix('.csv').name

            # Save the DataFrame to a CSV file. index=False is crucial.
            df.to_csv(output_file_path, index=False, encoding='utf-8')
            logger.info(f"Successfully transformed '{xml_file}' -> '{output_file_path}'")

        except Exception as e:
            logger.error(f"Failed to transform file '{xml_file}': {e}", exc_info=True)

    logger.info("ENTSO-E transformation completed.")