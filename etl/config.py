from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class APIConfig:
    metadata_url: str
    timeout_seconds: int = 30
    max_retries: int = 3


@dataclass
class IngestionConfig:
    reporting_packages_whitelist: list[str] = field(default_factory=list)
    organization_types: list[str] = field(default_factory=lambda: ["kunta", "kuntayhtymä"])
    dry_run_year_limit: int = 1
    dry_run_org_limit: int = 2


@dataclass
class StorageConfig:
    base_path: Path = Path("data")
    raw_path: Path = Path("data/raw")
    curated_path: Path = Path("data/curated")
    duckdb_path: Path = Path("data/vk_analytics.duckdb")


@dataclass
class AppConfig:
    api: APIConfig
    ingestion: IngestionConfig
    storage: StorageConfig
    log_level: str = "INFO"


def _coerce_scalar(value: str) -> Any:
    value = value.strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.isdigit():
        return int(value)
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    """A minimal YAML parser for this repository's simple config structure."""
    lines = path.read_text(encoding="utf-8").splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    for idx, raw in enumerate(lines):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        text = raw.strip()

        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()

        container = stack[-1][1]

        if text.startswith("- "):
            value = _coerce_scalar(text[2:])
            if not isinstance(container, list):
                raise ValueError(f"Invalid YAML list placement: {raw}")
            container.append(value)
            continue

        if ":" not in text:
            raise ValueError(f"Invalid YAML line: {raw}")

        key, value_part = text.split(":", 1)
        key = key.strip()
        value_part = value_part.strip()

        if value_part == "":
            next_container: Any = {}
            for next_idx in range(idx + 1, len(lines)):
                nxt = lines[next_idx]
                if not nxt.strip() or nxt.lstrip().startswith("#"):
                    continue
                next_indent = len(nxt) - len(nxt.lstrip(" "))
                if next_indent <= indent:
                    break
                if nxt.strip().startswith("- "):
                    next_container = []
                break
            if not isinstance(container, dict):
                raise ValueError(f"Cannot set key in list: {raw}")
            container[key] = next_container
            stack.append((indent, next_container))
        else:
            value = _coerce_scalar(value_part)
            if not isinstance(container, dict):
                raise ValueError(f"Cannot set key in list: {raw}")
            container[key] = value

    return root


def load_config(path: str | Path) -> AppConfig:
    config_dict = _load_yaml(Path(path))

    env_metadata_url = os.getenv("VK_METADATA_URL")
    if env_metadata_url:
        config_dict.setdefault("api", {})["metadata_url"] = env_metadata_url

    env_log_level = os.getenv("VK_LOG_LEVEL")
    if env_log_level:
        config_dict["log_level"] = env_log_level

    api = APIConfig(**config_dict.get("api", {}))
    ingestion = IngestionConfig(**config_dict.get("ingestion", {}))

    storage_dict = config_dict.get("storage", {})
    storage = StorageConfig(
        base_path=Path(storage_dict.get("base_path", "data")),
        raw_path=Path(storage_dict.get("raw_path", "data/raw")),
        curated_path=Path(storage_dict.get("curated_path", "data/curated")),
        duckdb_path=Path(storage_dict.get("duckdb_path", "data/vk_analytics.duckdb")),
    )

    cfg = AppConfig(
        api=api,
        ingestion=ingestion,
        storage=storage,
        log_level=config_dict.get("log_level", "INFO"),
    )
    cfg.storage.base_path.mkdir(parents=True, exist_ok=True)
    cfg.storage.raw_path.mkdir(parents=True, exist_ok=True)
    cfg.storage.curated_path.mkdir(parents=True, exist_ok=True)
    return cfg
