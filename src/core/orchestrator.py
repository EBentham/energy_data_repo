# python
import logging
import importlib
from datetime import date
from pathlib import Path
from typing import Union

from src.storage.file_handler import FileHandler
from src.connectors.base import BaseConnector
from src.config.models import AppConfig, load_config

logger = logging.getLogger(__name__)


class Orchestrator:
    """Orchestrates the ELT pipeline (dynamic transformer discovery).

    Responsibilities added:
    - validate config at initialization using Pydantic AppConfig
    - ensure connectors implement BaseConnector
    """

    def __init__(self, config: Union[str, dict, AppConfig]):
        # Accept a path to YAML, a dict, or an AppConfig instance
        if isinstance(config, str):
            app_cfg = load_config(config)
        elif isinstance(config, dict):
            app_cfg = AppConfig.parse_obj(config)
        elif isinstance(config, AppConfig):
            app_cfg = config
        else:
            raise TypeError("config must be a path, dict, or AppConfig instance")

        # store validated config as a plain dict for backward compatibility
        self.config = app_cfg.dict()
        self.file_handler = FileHandler()
        self.base_bronze_path = Path(self.config.get("storage", {}).get("bronze_path", "data/bronze"))
        self.base_silver_path = Path(self.config.get("storage", {}).get("silver_path", "data/silver"))

    def _load_connector(self, source_name: str, source_config: dict) -> BaseConnector | None:
        try:
            # allow explicit connector module in config later; default to convention
            module_path = source_config.get("connector", f"src.connectors.{source_name}")
            connector_module = importlib.import_module(module_path)
            class_name = f"{source_name.capitalize()}Connector"
            connector_class = getattr(connector_module, class_name, None)

            # fallback: search for any BaseConnector subclass in module
            if connector_class is None:
                for obj in vars(connector_module).values():
                    if isinstance(obj, type) and issubclass(obj, BaseConnector) and obj is not BaseConnector:
                        connector_class = obj
                        break

            if connector_class is None:
                logger.error("No connector class found for source '%s' in module '%s'", source_name, module_path)
                return None

            # Try instantiation with flexible signatures
            try:
                instance = connector_class(source_config)
            except TypeError:
                try:
                    instance = connector_class(source_name, source_config)
                except Exception as e:
                    logger.error("Failed to instantiate connector %s: %s", connector_class, e, exc_info=True)
                    return None

            if not isinstance(instance, BaseConnector):
                logger.error("Connector %s does not implement BaseConnector", connector_class)
                return None

            logger.info("Successfully loaded connector: '%s'", source_name)
            return instance

        except Exception as e:
            logger.error("Failed to load connector for '%s'. Error: %s", source_name, e, exc_info=True)
            return None

    def _load_transformer(self, source: str, query_name: str):
        """
        Dynamic loader for transformer modules.
        Convention: src/transformers/staging/{source}/{query_name}.py must expose `transform`.
        `transform(bronze_path: str, silver_path: str, config: dict) -> bool`
        """
        module_path = f"src.transformers.staging.{source}.{query_name}"
        try:
            mod = importlib.import_module(module_path)
            transform_fn = getattr(mod, "transform", None)
            if not callable(transform_fn):
                logger.warning("Module %s does not expose a callable `transform`", module_path)
                return None
            logger.info("Loaded transformer %s.%s", source, query_name)
            return transform_fn
        except ImportError as e:
            logger.warning("Transformer module not found: %s (%s)", module_path, e)
            return None
        except Exception as e:
            logger.error("Error loading transformer %s: %s", module_path, e, exc_info=True)
            return None

    def _extract_and_load(self, connector: BaseConnector, source_bronze_path: Path, start_date: date, end_date: date):
        logger.info("STAGE 1/2: EXTRACT & LOAD -> %s", source_bronze_path)
        source_bronze_path.mkdir(parents=True, exist_ok=True)
        raw_data_list = connector.extract(start_date, end_date)
        if raw_data_list:
            for raw in raw_data_list:
                self.file_handler.save_raw_data(raw, str(source_bronze_path))
        logger.info("Extraction complete")

    def _transform(self, source: str, source_bronze_path: Path, source_silver_path: Path, queries: list):
        logger.info("STAGE 2/2: TRANSFORM -> %s", source_silver_path)
        source_silver_path.mkdir(parents=True, exist_ok=True)

        for q in queries:
            transformer = self._load_transformer(source, q)
            if not transformer:
                logger.warning("Skipping missing transformer: %s.%s", source, q)
                continue

            try:
                logger.info("Running transformer %s.%s", source, q)
                success = transformer(str(source_bronze_path), str(source_silver_path), self.config)
                if not success:
                    logger.error("Transformer %s.%s reported failure", source, q)
            except Exception as e:
                logger.error("Transformer %s.%s crashed: %s", source, q, e, exc_info=True)

        logger.info("Transform stage complete")

    def run_pipeline(self, source: str, start_date: date, end_date: date, query: str = None):
        logger.info("Starting pipeline for %s [%s -> %s]", source, start_date, end_date)

        source_cfg = self.config.get("sources", {}).get(source)
        if not source_cfg:
            logger.error("Source %s not in config", source)
            return

        queries_cfg = source_cfg.get("queries", [])
        if query:
            queries_cfg = [q for q in queries_cfg if q.get("name") == query]
            if not queries_cfg:
                logger.error("Query %s not found for source %s", query, source)
                return

        connector = self._load_connector(source, source_cfg)
        if not connector:
            return

        bronze_path = self.base_bronze_path / source
        silver_path = self.base_silver_path / source

        self._extract_and_load(connector, bronze_path, start_date, end_date)

        query_names = [q.get("name") for q in queries_cfg]
        self._transform(source, bronze_path, silver_path, query_names)

        logger.info("Pipeline finished for %s", source)
