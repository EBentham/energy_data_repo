# tests/test_e2e_elexon_pipeline.py

import pytest
import os
import yaml
import pandas as pd
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

# Import the REAL Elexon components we want to test
from src.connectors.elexon import ElexonConnector
from src.storage.file_handler import FileHandler
from src.transformers.staging.elexon.registered_capacity import create_elexon_registered_capacity_silver

# --- Test Setup ---
pytestmark = pytest.mark.e2e
load_dotenv()


# --- The Test Function ---
def test_elexon_capacity_full_pipeline(tmp_path: Path):
    """
    Tests the full ELT pipeline for a single Elexon BMRS data type (Registered Capacity).
    1. EXTRACT: Calls the real BMRS API for capacity data.
    2. LOAD: Saves the raw XML to a temporary Bronze directory.
    3. TRANSFORM: Runs the staging transformer to create a Silver CSV.
    4. VERIFY: Checks the contents and structure of the final Silver CSV.
    """
    # Arrange:
    # 1. Get secrets and load configuration.
    api_key = os.getenv("ELEXON_API_KEY")
    if not api_key:
        pytest.skip("Skipping E2E test: ELEXON_API_KEY environment variable not set.")

    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r") as f:
        full_config = yaml.safe_load(f)

    elexon_config = full_config['sources']['elexon']
    elexon_config['api_key'] = api_key

    # Filter the config to only include the 'generation_registered_capacity' report.
    # This keeps the test focused, fast, and independent of other reports.
    elexon_config['reports'] = [
        r for r in elexon_config['reports'] if r['name'] == 'generation_registered_capacity'
    ]
    assert elexon_config['reports'], "Test setup failed: 'generation_registered_capacity' report not found in config."

    # 2. Define paths. Dates are not strictly needed for this non-time-series report,
    # but our extract method requires them, so we provide dummy values.
    start_date = date(2024, 1, 15)
    end_date = date(2024, 1, 15)
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    # 3. Instantiate the real components.
    connector = ElexonConnector(config=elexon_config)
    file_handler = FileHandler()

    # ===================================================================
    # STAGE 1 & 2: EXTRACT & LOAD (API -> Bronze Layer)
    # ===================================================================
    print("\n--- Running Elexon E&L Stage ---")
    # This makes the REAL network call to the BMRS API.
    raw_data_list = connector.extract(start_date, end_date)
    assert raw_data_list, "E&L FAILED: The connector should have extracted data."

    for raw_data in raw_data_list:
        file_handler.save_raw_data(raw_data, str(bronze_path / "elexon"))  # Pass the source-specific path

    raw_file = bronze_path / "elexon" / "generation_registered_capacity" / "generation_registered_capacity.xml"
    assert raw_file.exists(), f"E&L FAILED: Raw XML file was not created at {raw_file}"
    print(f"--- E&L Stage successful. Raw file created at {raw_file} ---")

    # ===================================================================
    # STAGE 3: TRANSFORM (Bronze -> Silver Layer)
    # ===================================================================
    print("\n--- Running Elexon Transform Stage ---")
    # Run the specific staging transformer for registered capacity.
    success = create_elexon_registered_capacity_silver(str(bronze_path / "elexon"), str(silver_path / "elexon"))
    assert success, "TRANSFORM FAILED: The transformation function returned False."

    # ===================================================================
    # STAGE 4: VERIFY (Check the Silver Layer Output)
    # ===================================================================
    print("\n--- Running Elexon Verify Stage ---")
    # 1. Check if the final Silver CSV file exists.
    expected_silver_file = silver_path / "elexon" / "dim_generation_units.csv"
    assert expected_silver_file.exists(), f"VERIFY FAILED: Silver CSV file was not created at {expected_silver_file}"

    # 2. Read the CSV and perform detailed checks.
    silver_df = pd.read_csv(expected_silver_file)

    assert not silver_df.empty, "VERIFY FAILED: The Silver CSV file is empty."

    expected_columns = ["bm_unit_id", "eic_code", "registered_capacity_mw", "fuel_type"]
    assert all(col in silver_df.columns for col in expected_columns), \
        f"VERIFY FAILED: Silver CSV is missing columns. Expected {expected_columns}, got {silver_df.columns.tolist()}"

    assert pd.api.types.is_numeric_dtype(silver_df['registered_capacity_mw']), \
        "VERIFY FAILED: 'registered_capacity_mw' column should be a numeric type."

    # Check for a known, large power station to ensure data integrity
    # For example, Drax is a major UK power station with multiple units (e.g., DRAXX-1)
    assert silver_df['bm_unit_id'].str.contains('DRAXX').any(), \
        "VERIFY FAILED: Expected to find a Drax power station unit ('DRAXX') in the data."

    print("\n✅✅✅ Elexon E2E Pipeline Test successful! ✅✅✅")