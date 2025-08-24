# tests/storage/test_file_handler.py

from pathlib import Path
from src.storage.file_handler import FileHandler, RawData

def test_save_raw_data_creates_file_and_directory(tmp_path: Path):
    """
    Tests that save_raw_data correctly creates the necessary subdirectory
    and writes the file with the correct content.

    Args:
        tmp_path (Path): A pytest fixture that provides a temporary directory Path object.
    """
    # Arrange: Set up the test data and components
    base_path = tmp_path
    file_handler = FileHandler()
    test_data = RawData(
        payload="<xml>This is a test</xml>",
        source_name="test_source",
        filename="2024-01-01.xml"
    )

    # Act: Call the method we want to test
    file_handler.save_raw_data(test_data, str(base_path))

    # Assert: Check that the outcome is what we expected
    expected_dir = base_path / "test_source"
    expected_file = expected_dir / "2024-01-01.xml"

    assert expected_dir.exists(), "The source-specific directory should be created."
    assert expected_dir.is_dir(), "The created path should be a directory."
    assert expected_file.exists(), "The data file should exist."
    assert expected_file.is_file(), "The created path should be a file."
    assert expected_file.read_text(encoding="utf-8") == "<xml>This is a test</xml>", \
        "The content of the file should match the payload."