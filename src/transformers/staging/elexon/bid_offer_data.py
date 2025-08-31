import pandas as pd
import logging
from pathlib import Path
from typing import List
from src.transformers.parsers.elexon_parser import parse_bid_offer_data

logger = logging.getLogger(__name__)


def create_elexon_bid_offer_silver(source_bronze_path: str, source_silver_path: str) -> bool:
    report_name = "bid_offer_level_data"
    bronze_report_path = Path(source_bronze_path) / report_name
    silver_report_path = Path(source_silver_path)

    xml_files = list(bronze_report_path.glob("**/*.xml"))
    if not xml_files: return True

    all_dfs: List[pd.DataFrame] = []
    for xml_file in xml_files:
        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                df = parse_bid_offer_data(f)
                if not df.empty: all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse file '{xml_file}': {e}")

    if not all_dfs: return True

    final_df = pd.concat(all_dfs).drop_duplicates().sort_values(by=['timestamp_utc', 'bm_unit_id'])
    output_file = silver_report_path / "fct_bid_offer_data.csv"
    final_df.to_csv(output_file, index=False)
    logger.info(f"Successfully created Silver table at '{output_file}'.")
    return True