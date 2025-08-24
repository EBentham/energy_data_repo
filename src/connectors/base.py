# src/connectors/base.py

from abc import ABC, abstractmethod
from datetime import date
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawData:
    """
    A data class to hold the raw payload from an API call along with metadata.
    Using frozen=True makes instances of this class immutable, which is a good
    practice for data that should not be changed after creation.

    Attributes:
        payload (str): The raw data content, typically XML or JSON text.
        source_name (str): The name of the source (e.g., 'entsoe').
        filename (str): The intended filename for saving this payload (e.g., '2024-10-26.xml').
    """
    payload: str
    source_name: str
    filename: str


class BaseConnector(ABC):
    """
    Abstract Base Class for all data source connectors.

    This class defines the standard interface (the "contract") that every
    connector must implement. This ensures that the orchestration layer can
    treat all connectors uniformly, simply calling the `extract` method
    to fetch data.
    """

    def __init__(self, name: str, config: dict):
        """
        Initializes the connector with its name and specific configuration.

        Args:
            name (str): The unique name of the data source (e.g., 'entsoe').
            config (dict): A dictionary containing the configuration for this
                           specific source, loaded from config.yaml.
        """
        self.name = name
        self.config = config
        logger.info(f"Initialized connector: '{self.name}'")

    @abstractmethod
    def extract(self, start_date: date, end_date: date) -> list[RawData]:
        """
        The core method for fetching data from the source API.

        This method must be implemented by all subclasses. It is responsible
        for handling the entire data extraction process for a given date range,
        including making API calls, handling pagination (if any), and returning
        the raw data in a standardized format.

        Args:
            start_date (date): The start date of the period to fetch data for.
            end_date (date): The end date of the period to fetch data for.

        Returns:
            list[RawData]: A list of RawData objects, where each object
                           represents a piece of data to be saved (e.g., a
                           single day's worth of data). An empty list should
                           be returned if no data is found or an error occurs.
        """
        pass