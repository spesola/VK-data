from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class APIConfig(BaseModel):
    metadata_url: str
    timeout_seconds: int = 30
    max_retries: int = 3


class IngestionConfig(BaseModel):
    reporting_packages_whitelist: list[str] = Field(default_factory=list)
    organization_types: list[str] = Field(default_factory=lambda: ["kunta", "kuntayhtymä"])
    dry_run_year_limit: int = 1
    dry_run_org_limit: int = 2


class StorageConfig(BaseModel):
    base_path: Path = Path("data")
    raw_path: Path = Path("data/raw")
    curated_path: Path = Path("data/curated")
    duckdb_path: Path = Path("data/vk_analytics.duckdb")


class AppConfig(BaseModel):
    api: APIConfig
    ingestion: IngestionConfig
    storage: StorageConfig
    log_level: str = "INFO"


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(path: str | Path) -> AppConfig:
    config_dict = _load_yaml(Path(path))

    env_metadata_url = os.getenv("VK_METADATA_URL")
    if env_metadata_url:
        config_dict.setdefault("api", {})["metadata_url"] = env_metadata_url

    env_log_level = os.getenv("VK_LOG_LEVEL")
    if env_log_level:
        config_dict["log_level"] = env_log_level

    cfg = AppConfig.model_validate(config_dict)
    cfg.storage.base_path.mkdir(parents=True, exist_ok=True)
    cfg.storage.raw_path.mkdir(parents=True, exist_ok=True)
    cfg.storage.curated_path.mkdir(parents=True, exist_ok=True)
    return cfg
