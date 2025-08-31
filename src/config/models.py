from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, root_validator


class QueryConfig(BaseModel):
    name: str
    params: Optional[Dict[str, Any]] = None
    documentType: Optional[str] = None
    domain_params: Optional[Dict[str, str]] = None


class SourceConfig(BaseModel):
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    queries: List[QueryConfig] = []

    class Config:
        extra = "allow"


class StorageConfig(BaseModel):
    bronze_path: str = "data/bronze"
    silver_path: str = "data/silver"
    gold_path: Optional[str] = None


class AppConfig(BaseModel):
    sources: Dict[str, SourceConfig]
    storage: StorageConfig = StorageConfig()
    logging: Optional[Dict[str, Any]] = None
    dbt: Optional[Dict[str, Any]] = None

    @root_validator
    def check_sources_not_empty(cls, values):
        if not values.get("sources"):
            raise ValueError("`sources` must contain at least one source")
        return values


def load_config(path: str) -> AppConfig:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return AppConfig.parse_obj(data)

