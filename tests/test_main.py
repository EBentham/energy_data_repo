# tests/test_main.py

from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from datetime import date

# Import the CLI function from your main script
from src.main import cli
from src.connectors.base import RawData

@patch('src.main.FileHandler.save_raw_data') # Mock the file saving to not touch the disk
@patch('src.main.load_connector')           # Mock the connector loading
def test_extract_cli_command_success(mock_load_connector: MagicMock, mock_save_raw_data: MagicMock):
    """
    Tests the `extract` CLI command from end-to-end.
    Mocks are used to isolate the CLI logic from the actual connector and file system.
    """
    # Arrange
    # 1. Create a fake connector object that the mock loader will return.
    mock_connector = MagicMock()
    # 2. Configure its `extract` method to return some predefined fake data.
    fake_raw_data = [
        RawData(payload="<xml>data1</xml>", source_name="entsoe", filename="2024-01-01.xml")
    ]
    mock_connector.extract.return_value = fake_raw_data
    # 3. Tell the mock `load_connector` function to return our fake connector.
    mock_load_connector.return_value = mock_connector

    # 4. Instantiate the Click test runner.
    runner = CliRunner()

    # Act: Invoke the CLI command as if a user typed it in the terminal.
    result = runner.invoke(cli, [
        'extract',
        '--source', 'entsoe',
        '--start', '2024-01-01',
        '--end', '2024-01-01'
    ])

    # Assert
    # 1. Check that the command exited cleanly.
    assert result.exit_code == 0
    assert "--- Starting EXTRACT job for source: 'entsoe' ---" in result.output
    assert "--- EXTRACT job for source: 'entsoe' completed successfully. ---" in result.output

    # 2. Verify that `load_connector` was called with the correct source name.
    mock_load_connector.assert_called_with('entsoe', unittest.mock.ANY)

    # 3. Verify that the `extract` method on our fake connector was called with the correct dates.
    mock_connector.extract.assert_called_once_with(
        date(2024, 1, 1),
        date(2024, 1, 1)
    )

    # 4. Verify that the file saver was called once with our fake data.
    mock_save_raw_data.assert_called_once_with(fake_raw_data[0], unittest.mock.ANY)