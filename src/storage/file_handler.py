# src/storage/file_handler.py

import logging
from pathlib import Path
from src.connectors.base import RawData  # Assuming base.py is now in src/connectors/

logger = logging.getLogger(__name__)


class FileHandler:
    """Handles saving raw data payloads to a specified directory."""

    @staticmethod
    def save_raw_data(raw_data: RawData, output_dir_str: str):
        """
        Saves a single RawData payload to the specified output directory.

        Args:
            raw_data (RawData): The data object containing the payload and a
                                relative path in the 'filename' attribute.
            output_dir_str (str): The full path to the directory where data should be saved
                                  (e.g., 'data/bronze/entsoe').
        """
        if not isinstance(raw_data.payload, str) or not raw_data.payload:
            logger.warning(f"Skipping save for '{raw_data.filename}' due to empty payload.")
            return

        try:
            # The output directory is now passed in fully formed.
            output_dir = Path(output_dir_str)

            # The full path is the output directory joined with the relative path from RawData.
            full_path = output_dir / raw_data.filename

            # Ensure the parent directory for the file exists before writing.
            full_path.parent.mkdir(parents=True, exist_ok=True)

            with open(full_path, "w", encoding="utf-8") as f:
                f.write(raw_data.payload)

            logger.info(f"Successfully saved raw data to: {full_path}")

        except Exception as e:
            logger.error(f"An unexpected error occurred during file save for '{raw_data.filename}': {e}")