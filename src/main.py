from datetime import datetime
import logging
from pathlib import Path
from typing import Optional

import click
import yaml

from src.core.logging import setup_logging
from src.core.orchestrator import Orchestrator


# --- Application Setup ---
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

with open(CONFIG_PATH, "r") as f:
    _CONFIG = yaml.safe_load(f)

setup_logging(level=_CONFIG.get("logging", {}).get("level", "INFO"))
logger = logging.getLogger(__name__)


def main(source: str, start: str, end: str, query: Optional[str] = None) -> bool:
    """Run pipeline using string inputs for the four CLI parameters.

    Parameters:
        source: data source name (e.g., 'entsoe', 'elexon')
        start: start date string YYYY-MM-DD
        end: end date string YYYY-MM-DD
        query: optional query name to run (e.g., 'generation_per_type')

    Returns:
        True on success, False on error.
    """
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD for start and end.")
        return False

    try:
        orchestrator = Orchestrator(_CONFIG)
        orchestrator.run_pipeline(source, start_date, end_date, query)
        return True
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return False


# --- CLI wrapper for backward compatibility ---
@click.group()
def cli():
    """A CLI for the Energy Data Platform ELT pipeline."""


@cli.command()
@click.option("--source", required=True, type=str, help="The data source to run (e.g., entsoe).")
@click.option("--start", required=True, help="Start date in YYYY-MM-DD format.")
@click.option("--end", required=True, help="End date in YYYY-MM-DD format.")
@click.option("--query", default=None, help="Optional: Run for a single query name (e.g., generation_per_type).")
def run(source: str, start: str, end: str, query: Optional[str]):
    """
    Runs the full ELT pipeline for a given data source and date range (click wrapper).
    """
    success = main(source, start, end, query)
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    cli()

    # python -m src.main run --source entsoe --start 2025-03-01 --end 2025-03-01

    # TODO:
    # 1. Build out tests for validation URL's
    # 2. Add ELEXON data sources.