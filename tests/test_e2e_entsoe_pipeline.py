# tests/test_e2e_entsoe_pipeline.py

import pytest
import os
import yaml
import pandas as pd
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

# Import the REAL components we want to test together
from src.connectors.entsoe import EntsoeConnector
from src.storage.file_handler import FileHandler
# Import the NEW staging transformer function we want to test
from src.transformers.staging.entsoe.stg_entsoe_generation import create_entsoe_generation_silver

# --- Test Setup ---
# Mark the entire file as containing end-to-end tests
pytestmark = pytest.mark.e2e

# Load environment variables from .env file for this test session
load_dotenv()


# --- The Test Function ---
def test_entsoe_generation_full_pipeline(tmp_path: Path):
    """
    Tests the full ELT pipeline for a single ENTSO-E data type (Generation).
    1. EXTRACT: Calls the real ENTSO-E API for generation data.
    2. LOAD: Saves the raw XML to a temporary Bronze directory.
    3. TRANSFORM: Runs the staging transformer to create a Silver CSV.
    4. VERIFY: Checks the contents and structure of the final Silver CSV.
    """
    # Arrange:
    # 1. Get secrets and load configuration, same as before.
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        pytest.skip("Skipping E2E test: ENTSO_E_API_KEY environment variable not set.")

    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r") as f:
        full_config = yaml.safe_load(f)

    entsoe_config = full_config['sources']['entsoe']
    entsoe_config['api_key'] = api_key

    # We only want to test one query type to keep the test focused and fast.
    # Filter the config to only include the 'generation_per_type' query.
    entsoe_config['queries'] = [
        q for q in entsoe_config['queries'] if q['name'] == 'generation_per_type'
    ]
    assert entsoe_config['queries'], "Test setup failed: 'generation_per_type' query not found in config."

    # 2. Define date range and paths within the temporary directory.
    start_date = date(2024, 1, 15)
    end_date = date(2024, 1, 15)
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"

    # 3. Instantiate the real components.
    connector = EntsoeConnector(config=entsoe_config)
    file_handler = FileHandler()

    # ===================================================================
    # STAGE 1 & 2: EXTRACT & LOAD (API -> Bronze Layer)
    # ===================================================================
    print("\n--- Running E&L Stage ---")
    # This makes the REAL network call.
    raw_data_list = connector.extract(start_date, end_date)
    assert raw_data_list, "E&L FAILED: The connector should have extracted data."

    for raw_data in raw_data_list:
        file_handler.save_raw_data(raw_data, str(bronze_path))

    # Verify that the raw XML file was created
    raw_file = bronze_path / "entsoe" / "generation_per_type" / "2024-01-15.xml"
    assert raw_file.exists(), f"E&L FAILED: Raw XML file was not created at {raw_file}"
    print(f"--- E&L Stage successful. Raw file created at {raw_file} ---")

    # ===================================================================
    # STAGE 3: TRANSFORM (Bronze -> Silver Layer)
    # ===================================================================
    print("\n--- Running Transform Stage ---")
    # Run the specific staging transformer we are testing.
    success = create_entsoe_generation_silver(str(bronze_path), str(silver_path))
    assert success, "TRANSFORM FAILED: The transformation function returned False."

    # ===================================================================
    # STAGE 4: VERIFY (Check the Silver Layer Output)
    # ===================================================================
    print("\n--- Running Verify Stage ---")
    # 1. Check if the final Silver CSV file exists.
    expected_silver_file = silver_path / "entsoe" / "generation_per_type" / "generation.csv"
    assert expected_silver_file.exists(), f"VERIFY FAILED: Silver CSV file was not created at {expected_silver_file}"

    # 2. Read the CSV and perform detailed checks on its content and schema.
    silver_df = pd.read_csv(expected_silver_file, parse_dates=["timestamp_utc"])

    # Check for non-empty DataFrame
    assert not silver_df.empty, "VERIFY FAILED: The Silver CSV file is empty."

    # Check for expected columns
    expected_columns = ["timestamp_utc", "fuel_type", "generation_mw"]
    assert all(col in silver_df.columns for col in expected_columns), \
        f"VERIFY FAILED: Silver CSV is missing columns. Expected {expected_columns}, got {silver_df.columns.tolist()}"

    # Check data types
    assert pd.api.types.is_datetime64_any_dtype(silver_df['timestamp_utc']), \
        "VERIFY FAILED: 'timestamp_utc' column should be a datetime type."
    assert pd.api.types.is_numeric_dtype(silver_df['generation_mw']), \
        "VERIFY FAILED: 'generation_mw' column should be a numeric type."

    # Check for a known value (this makes the test more robust)
    # For example, let's check that wind power was reported.
    assert "B19" in silver_df['fuel_type'].unique(), \
        "VERIFY FAILED: Expected to find 'B19' (Wind) in the fuel_type column."

    print("\n✅✅✅ E2E Pipeline Test successful! ✅✅✅")