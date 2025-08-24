# tests/connectors/test_entsoe.py

import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import date

from src.connectors.entsoe import EntsoeConnector

# A sample config for initializing the connector in tests
DUMMY_CONFIG = {
    'api_key': 'fake_api_key',
    'bidding_zone': '10Y-GB'
}

@patch('src.connectors.entsoe.requests.Session.get')
def test_extract_success(mock_get: MagicMock):
    """
    Tests the success case where the API returns valid data for a single day.
    """
    # Arrange: Configure the mock to simulate a successful API call
    mock_response = MagicMock()
    mock_response.text = "<xml>Success</xml>"
    mock_response.raise_for_status.return_value = None  # Simulate a 200 OK response
    mock_get.return_value = mock_response

    connector = EntsoeConnector(config=DUMMY_CONFIG)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 1)

    # Act: Run the extraction
    result = connector.extract(start_date, end_date)

    # Assert: Verify the outcome
    mock_get.assert_called_once()  # Ensure the API was called exactly once
    assert len(result) == 1
    assert result[0].source_name == "entsoe"
    assert result[0].payload == "<xml>Success</xml>"
    assert result[0].filename == "2024-01-01.xml"

@patch('src.connectors.entsoe.requests.Session.get')
def test_extract_handles_api_failure_gracefully(mock_get: MagicMock):
    """
    Tests the failure case where the API call raises an exception.
    The connector should handle this gracefully and return an empty list.
    """
    # Arrange: Configure the mock to simulate a network error
    mock_get.side_effect = requests.exceptions.RequestException("API connection failed")

    connector = EntsoeConnector(config=DUMMY_CONFIG)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 1)

    # Act: Run the extraction
    result = connector.extract(start_date, end_date)

    # Assert: Verify that the function returns an empty list and doesn't crash
    mock_get.assert_called_once()
    assert result == [], "The result should be an empty list on API failure."

def test_init_raises_error_if_no_api_key():
    """
    Tests that the connector raises a ValueError if the API key is missing.
    """
    # Arrange
    config_without_key = {'bidding_zone': '10Y-GB'}

    # Act & Assert
    with pytest.raises(ValueError, match="ENTSO-E API key is required"):
        EntsoeConnector(config=config_without_key)