# src/core/logging.py

import logging
import sys

def setup_logging(level: str = "INFO", format_str: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"):
    """
    Sets up centralized logging for the application.

    This function configures the root logger, which means all loggers created
    in other modules (via `logging.getLogger(__name__)`) will inherit this
    configuration.

    Args:
        level (str): The minimum logging level to output (e.g., "DEBUG", "INFO", "WARNING").
                     Defaults to "INFO".
        format_str (str): The format string for log messages. Defaults to a standard
                          professional format.
    """
    # Get the actual logging level constant (e.g., logging.INFO) from the string.
    # The `getattr` function provides a safe way to do this with a default fallback.
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure the root logger. Using `force=True` is good practice in complex
    # applications to ensure this configuration is applied even if a library
    # has already tried to configure logging.
    logging.basicConfig(
        level=log_level,
        format=format_str,
        # Handlers specify where log messages are sent. Here we send them to the console.
        handlers=[
            logging.StreamHandler(sys.stdout)
            # You could easily add a FileHandler here to also log to a file:
            # logging.FileHandler("app.log")
        ],
        force=True
    )

    # Log a confirmation message so we know this setup ran successfully.
    logging.getLogger(__name__).info(f"Logging configured successfully with level {level}.")