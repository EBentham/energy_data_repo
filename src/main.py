import click
import yaml
import importlib
from datetime import datetime
import logging
from pathlib import Path

from src.core.logging import setup_logging
from src.storage.file_handler import FileHandler

# --- Transformer Function Imports ---
from src.transformers.staging.entsoe.stg_entsoe_generation import create_entsoe_generation_silver
from src.transformers.staging.entsoe.stg_entsoe_load import create_entsoe_load_silver
from src.transformers.staging.entsoe.stg_entsoe_prices import create_entsoe_prices_silver

# --- Application Setup ---
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

setup_logging(level=config.get('logging', {}).get('level', 'INFO'))
logger = logging.getLogger(__name__)

# --- Transformer Mapping ---
TRANSFORMER_MAP = {
    'entsoe': {
        'generation_per_type': create_entsoe_generation_silver,
        'total_load': create_entsoe_load_silver,
        'day_ahead_prices': create_entsoe_prices_silver,
    }
}


# --- Helper Functions ---
def load_connector(source_name: str, source_config: dict):
    """Dynamically loads a data source connector class."""
    try:
        module_path = f"src.connectors.{source_name}"
        connector_module = importlib.import_module(module_path)
        class_name = f"{source_name.capitalize()}Connector"
        connector_class = getattr(connector_module, class_name)
        logger.info(f"Successfully loaded connector: '{source_name}'")
        return connector_class(source_config)
    except Exception as e:
        logger.error(f"Failed to load connector for '{source_name}'. Error: {e}", exc_info=True)
        return None


# --- CLI Command Group ---
@click.group()
def cli():
    """A CLI for the Energy Data Platform ELT pipeline."""
    pass


@cli.command()
@click.option('--source', required=True, type=str, help='The data source to run (e.g., entsoe).')
@click.option('--start', required=True, help='Start date in YYYY-MM-DD format.')
@click.option('--end', required=True, help='End date in YYYY-MM-DD format.')
@click.option('--query', default=None, help='Optional: Run for a single query name (e.g., generation_per_type).')
def run(source, start, end, query):
    """Runs the full ELT pipeline for a given data source and date range."""
    logger.info(f"--- Starting Full ELT Pipeline Run for source: '{source}' ---")

    # --- ARRANGE STAGE ---
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d').date()
        end_date = datetime.strptime(end, '%Y-%m-%d').date()
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD.");
        return

    source_config = config['sources'].get(source)
    if not source_config:
        logger.error(f"Source '{source}' not found in config.yaml.");
        return

    if query:
        source_config['queries'] = [q for q in source_config['queries'] if q['name'] == query]
        if not source_config['queries']:
            logger.error(f"Query '{query}' not found in config for source '{source}'.");
            return
        logger.info(f"Running pipeline for single query: '{query}'")

    connector = load_connector(source, source_config)
    if not connector: return

    file_handler = FileHandler()
    # --- PATH CONSTRUCTION LOGIC ---
    # The orchestrator is now responsible for creating the full, source-specific paths.
    base_bronze_path = Path(config.get('storage', {}).get('bronze_path', 'data/bronze'))
    base_silver_path = Path(config.get('storage', {}).get('silver_path', 'data/silver'))

    source_bronze_path = base_bronze_path / source
    source_silver_path = base_silver_path / source

    # --- STAGE 1 & 2: EXTRACT & LOAD ---
    logger.info(f"\n--- STAGE 1/2: EXTRACT & LOAD to '{source_bronze_path}' ---")
    raw_data_list = connector.extract(start_date, end_date)
    if raw_data_list:
        # Pass the full source-specific path to the handler
        for raw_data in raw_data_list:
            file_handler.save_raw_data(raw_data, str(source_bronze_path))
    logger.info("--- E&L Stage Complete ---")

    # --- STAGE 3: TRANSFORM ---
    logger.info(f"\n--- STAGE 3: TRANSFORM to '{source_silver_path}' ---")
    source_transformers = TRANSFORMER_MAP.get(source, {})
    queries_to_transform = [q['name'] for q in source_config['queries']]

    for query_name in queries_to_transform:
        transformer_func = source_transformers.get(query_name)
        if transformer_func:
            logger.info(f">>> Running transformer for: '{query_name}'")
            # Pass the full source-specific paths to the transformer
            success = transformer_func(str(source_bronze_path), str(source_silver_path))
            if not success:
                logger.error(f"Transformer for '{query_name}' failed.")
        else:
            logger.warning(f"No transformer found for query '{query_name}'. Skipping.")

    logger.info("--- Transform Stage Complete ---")
    logger.info(f"\n--- Full ELT Pipeline Run for source: '{source}' Finished ---")


if __name__ == '__main__':
    cli()