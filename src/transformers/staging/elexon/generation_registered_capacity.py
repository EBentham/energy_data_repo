# Wrapper transformer to provide a standard `transform(bronze_path, silver_path, config)` API
from src.transformers.staging.elexon.registered_capacity import create_elexon_registered_capacity_silver


def transform(bronze_path: str, silver_path: str, config: dict) -> bool:
    return create_elexon_registered_capacity_silver(bronze_path, silver_path)

