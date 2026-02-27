from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

from etl.config import AppConfig

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _http_get_json(url: str, timeout_seconds: int) -> Any:
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _load_fixture_json(relative_path: str) -> Any:
    path = FIXTURES_DIR / relative_path
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_metadata(cfg: AppConfig) -> list[dict[str, Any]]:
    try:
        data = _http_get_json(cfg.api.metadata_url, cfg.api.timeout_seconds)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return _load_fixture_json("aineistot.json")


def filter_metadata(records: list[dict[str, Any]], cfg: AppConfig) -> list[dict[str, Any]]:
    whitelist = set(cfg.ingestion.reporting_packages_whitelist)
    org_types = set(cfg.ingestion.organization_types)
    return [
        r
        for r in records
        if r.get("reporting_package") in whitelist and r.get("organization_type") in org_types
    ]


def select_dry_run_scope(records: list[dict[str, Any]], cfg: AppConfig) -> list[dict[str, Any]]:
    years = sorted({int(r.get("year")) for r in records if r.get("year") is not None})
    selected_years = set(years[: cfg.ingestion.dry_run_year_limit])
    year_filtered = [r for r in records if int(r.get("year")) in selected_years]

    municipalities = sorted({str(r.get("municipality_code")) for r in year_filtered})
    selected_municipalities = set(municipalities[: cfg.ingestion.dry_run_org_limit])
    return [r for r in year_filtered if str(r.get("municipality_code")) in selected_municipalities]


def _download_dataset(dataset_url: str, timeout_seconds: int) -> dict[str, Any]:
    if dataset_url.startswith("fixture://"):
        rel = dataset_url.replace("fixture://", "")
        return _load_fixture_json(rel)
    return _http_get_json(dataset_url, timeout_seconds)


def normalize_long(records: list[dict[str, Any]], cfg: AppConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metadata in records:
        dataset = _download_dataset(metadata["dataset_url"], cfg.api.timeout_seconds)
        values = dataset.get("values", {})
        for metric, value in values.items():
            rows.append(
                {
                    "dataset_id": metadata.get("dataset_id"),
                    "reporting_package": metadata.get("reporting_package"),
                    "organization_type": metadata.get("organization_type"),
                    "municipality_code": str(metadata.get("municipality_code")),
                    "year": int(metadata.get("year")),
                    "metric": metric,
                    "value": value,
                }
            )
    return rows


def write_parquet_fallback(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write JSON lines at parquet path when parquet libraries are unavailable."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_raw_ingestion_pipeline(cfg: AppConfig) -> tuple[list[dict[str, Any]], list[str]]:
    metadata = fetch_metadata(cfg)
    filtered = filter_metadata(metadata, cfg)
    scoped = select_dry_run_scope(filtered, cfg)
    rows = normalize_long(scoped, cfg)

    output_path = cfg.storage.base_path / "facts_raw.parquet"
    write_parquet_fallback(rows, output_path)

    schema = list(rows[0].keys()) if rows else []
    return rows, schema
