import pandas as pd
import logging
from pathlib import Path
from src.transformers.parsers.elexon_parser import parse_registered_capacity

logger = logging.getLogger(__name__)


def create_elexon_registered_capacity_silver(source_bronze_path: str, source_silver_path: str) -> bool:
    report_name = "generation_registered_capacity"
    bronze_report_path = Path(source_bronze_path) / report_name
    silver_report_path = Path(source_silver_path)

    # Accept both XML and JSON raw payloads
    raw_files = list(bronze_report_path.glob("*.xml")) + list(bronze_report_path.glob("*.json"))
    if not raw_files:
        logger.warning(f"No raw files found for '{report_name}'. Skipping.")
        return True

    # This report is not time-series, so we expect only one file
    xml_file = raw_files[0]
    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            df = parse_registered_capacity(f)

        if df.empty:
            logger.warning(f"Parsing resulted in an empty DataFrame for '{report_name}'.")
            return True

        output_file = silver_report_path / "dim_generation_units.csv"  # Using a dimensional model name
        df.sort_values(by='bm_unit_id').to_csv(output_file, index=False)
        logger.info(f"Successfully created Silver table at '{output_file}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to transform '{report_name}': {e}")
        return False