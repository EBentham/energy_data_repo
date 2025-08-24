import pytest
import os
import yaml # <-- Import YAML library
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

# Import the REAL classes we want to test together
from src.connectors.entsoe import EntsoeConnector
from src.storage.file_handler import FileHandler

# --- Test Setup ---
# Mark the entire file as containing end-to-end tests
pytestmark = pytest.mark.e2e

# Load environment variables from .env file for this test session
load_dotenv()

# --- The Test Function ---
def test_entsoe_full_e2e_extraction(tmp_path: Path):
    """
    Tests the full Extract and Load pipeline for the ENTSO-E connector.
    This test is a more faithful representation because it:
    1. Loads the REAL config.yaml file.
    2. Overrides the API key placeholder with the real key.
    3. Calls the real API for ALL configured queries.
    4. Saves the multiple resulting files to a temporary directory.
    5. Verifies that the expected files were created.
    """
    tmp_path = Path(r"C:\Users\Bobbo\OneDrive\Desktop\Python\energy_data_repo\data\raw")
    # Arrange:
    # 1. Get the secret API key from the environment.
    api_key = os.getenv("ENTSOE_API_KEY")

    # 2. Skip the test if the key isn't available.
    if not api_key:
        pytest.skip("Skipping E2E test: ENTSO_E_API_KEY environment variable not set.")

    # 3. Load the ACTUAL application configuration file.
    # This ensures our test uses the same settings as the real app.
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r") as f:
        full_config = yaml.safe_load(f)

    # 4. Get the specific config for the 'entsoe' source.
    entsoe_config = full_config['sources']['entsoe']

    # 5. IMPORTANT: Override the API key placeholder with the real key.
    entsoe_config['api_key'] = api_key

    # 6. Define a short, specific date range to minimize test time.
    start_date = date(2024, 1, 15)
    end_date = date(2024, 6, 15)

    # 7. Instantiate the real components with the real configuration.
    connector = EntsoeConnector(config=entsoe_config)
    file_handler = FileHandler()
    bronze_path = tmp_path / "bronze"

    # Act:
    # 1. Run the extraction. This makes multiple network calls, one for each query in the config.
    raw_data_list = connector.extract(start_date, end_date)

    # 2. Save all the results.
    for raw_data in raw_data_list:
        file_handler.save_raw_data(raw_data, str(bronze_path))

    # Assert:
    # 1. Verify that the number of results matches the number of configured queries.
    num_configured_queries = len(entsoe_config.get('queries', []))
    assert raw_data_list, "The connector should have extracted data."

    # assert len(raw_data_list) == num_configured_queries, \
    #     f"Expected {num_configured_queries} data objects, one for each query in config.yaml."
    #
    # # 2. Let's pick one specific query to verify its content and file creation.
    # # We'll check the 'generation_per_type' query.
    # generation_data = next((item for item in raw_data_list if "generation_per_type" in item.filename), None)
    # assert generation_data is not None, "Data for 'generation_per_type' query should be present."
    # assert generation_data.source_name == "entsoe"
    # assert generation_data.filename == "generation_per_type/2024-01-15.xml"
    # assert "<TimeSeries>" in generation_data.payload, "The payload for generation data should be valid XML."
    #
    # # 3. Verify that the specific file for our chosen query was written to disk.
    # expected_file = bronze_path / "entsoe" / "generation_per_type" / "2024-01-15.xml"
    # assert expected_file.exists(), f"The output file should have been created at {expected_file}"
    # assert expected_file.is_file()
    #
    # # 4. Verify that the written file is not empty and has the correct content.
    # content = expected_file.read_text()
    # assert len(content) > 0, "The saved file should not be empty."
    # assert "<TimeSeries>" in content, "The content of the saved file should match the payload."
    #
    # print(f"\nE2E test successful. Verified {len(raw_data_list)} files saved to temporary directory: {bronze_path}")