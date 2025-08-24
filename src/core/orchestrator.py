# src/core/orchestrator.py

import subprocess
import logging
from pathlib import Path

# Get a logger for this module
logger = logging.getLogger(__name__)

class DbtOrchestrator:
    """
    A Python wrapper to orchestrate transformation jobs using dbt-core.

    This class is responsible for constructing and executing `dbt` commands
    as subprocesses, leveraging the power of dbt for DAG management and
    execution.
    """

    def __init__(self, dbt_project_dir: str):
        """
        Initializes the orchestrator with the path to the dbt project.

        Args:
            dbt_project_dir (str): The file path to the directory containing
                                   the `dbt_project.yml` file.

        Raises:
            FileNotFoundError: If the specified dbt project directory does not exist.
        """
        self.dbt_project_dir = Path(dbt_project_dir)
        if not self.dbt_project_dir.is_dir():
            raise FileNotFoundError(
                f"dbt project directory not found at: {self.dbt_project_dir.resolve()}"
            )
        logger.info(f"DbtOrchestrator initialized for project: {self.dbt_project_dir.resolve()}")

    def run_transformations(self, select: str = None) -> bool:
        """
        Runs the dbt transformation pipeline.

        This method builds and executes the `dbt run` command. It streams dbt's
        output directly to the console in real-time.

        Args:
            select (str, optional): A dbt model selection string (e.g.,
                                    'fct_daily_market_summary', '+stg_entsoe_generation').
                                    If None, dbt will run all models. Defaults to None.

        Returns:
            bool: True if the dbt command executed successfully, False otherwise.
        """
        command = ["dbt", "run"]
        if select:
            command.extend(["--select", select])

        logger.info(f"Executing dbt command: `{' '.join(command)}`")

        try:
            # Use subprocess.run to execute the dbt command.
            # - cwd: Sets the working directory, which is crucial so dbt can find dbt_project.yml.
            # - check=True: Raises a CalledProcessError if dbt returns a non-zero exit code (i.e., fails).
            # - text=True: Ensures stdout/stderr are decoded as text.
            # - By not capturing stdout/stderr, their output is automatically streamed to the parent's
            #   console, providing real-time feedback.
            subprocess.run(
                command,
                cwd=self.dbt_project_dir,
                check=True,
                text=True,
            )
            logger.info("dbt transformations completed successfully.")
            return True
        except FileNotFoundError:
            logger.error(
                "'dbt' command not found. Is dbt-core installed in your environment and in your system's PATH?"
            )
            return False
        except subprocess.CalledProcessError as e:
            # The dbt error output is already on the console, so we just need to log the failure.
            logger.error(f"dbt command failed with exit code {e.returncode}.")
            return False