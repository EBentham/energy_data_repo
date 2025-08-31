# Wrapper transformer to provide a standard `transform(bronze_path, silver_path, config)` API
from src.transformers.staging.entsoe.stg_entsoe_generation import create_entsoe_generation_silver


def transform(bronze_path: str, silver_path: str, config: dict) -> bool:
    return create_entsoe_generation_silver(bronze_path, silver_path)

