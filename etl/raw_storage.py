from __future__ import annotations

import json
import re
from itertools import product
from pathlib import Path
from typing import Any

import polars as pl


_DIM_SANITIZE_RE = re.compile(r"[^a-z0-9]+")


class DatasetFormatError(ValueError):
    """Raised when dataset payload does not match the expected JSON-stat-like shape."""


def _sanitize_column_name(name: str) -> str:
    lowered = name.strip().lower()
    return _DIM_SANITIZE_RE.sub("_", lowered).strip("_")


def _extract_dataset(payload: dict[str, Any]) -> dict[str, Any]:
    dataset = payload.get("dataset", payload)
    if not isinstance(dataset, dict):
        raise DatasetFormatError("Dataset must be a JSON object")
    if "dimension" not in dataset or "value" not in dataset:
        raise DatasetFormatError("Dataset must include 'dimension' and 'value'")
    return dataset


def normalize_dataset_json(payload: dict[str, Any]) -> pl.DataFrame:
    """Convert a single dataset JSON into long-form tabular rows."""
    dataset = _extract_dataset(payload)
    dimensions: dict[str, Any] = dataset["dimension"]
    dim_order: list[str] = dataset.get("id") or list(dimensions.keys())

    dim_codes: list[list[str]] = []
    dim_labels: list[dict[str, str]] = []
    sanitized_dim_names: list[str] = []

    for dim_name in dim_order:
        dim_payload = dimensions[dim_name]
        category = dim_payload.get("category", {})
        index = category.get("index", {})
        labels = category.get("label", {})

        if isinstance(index, list):
            codes = [str(code) for code in index]
        elif isinstance(index, dict):
            codes = [key for key, _ in sorted(index.items(), key=lambda kv: int(kv[1]))]
        else:
            raise DatasetFormatError(f"Dimension '{dim_name}' has unsupported category.index")

        dim_codes.append(codes)
        dim_labels.append({str(code): str(labels.get(code, code)) for code in codes})
        sanitized_dim_names.append(_sanitize_column_name(dim_name))

    combinations = list(product(*dim_codes))
    raw_values = dataset["value"]

    value_map: dict[int, Any]
    if isinstance(raw_values, list):
        value_map = {idx: raw_values[idx] for idx in range(min(len(raw_values), len(combinations)))}
    elif isinstance(raw_values, dict):
        value_map = {int(idx): value for idx, value in raw_values.items()}
    else:
        raise DatasetFormatError("Dataset value must be a list or object")

    rows: list[dict[str, Any]] = []
    for idx, combo in enumerate(combinations):
        row: dict[str, Any] = {}
        for dim_idx, code in enumerate(combo):
            column = sanitized_dim_names[dim_idx]
            row[column] = code
            row[f"{column}_label"] = dim_labels[dim_idx].get(code, code)
        row["value"] = value_map.get(idx)
        rows.append(row)

    return pl.DataFrame(rows)


def apply_dry_run_limits(
    frame: pl.DataFrame,
    *,
    reporting_package_limit: int = 1,
    organization_limit: int = 2,
    year_limit: int = 1,
) -> pl.DataFrame:
    """Apply dry-run limits (1 reporting package, 2 organizations, 1 year)."""

    selectors = [
        ("reporting package", reporting_package_limit, ["raportointikokonaisuus", "reporting_package"]),
        ("organization", organization_limit, ["organisaatio", "organization", "kunta"]),
        ("year", year_limit, ["vuosi", "year"]),
    ]

    filtered = frame
    for _, limit, candidates in selectors:
        if limit <= 0:
            continue

        matching = [
            col
            for col in filtered.columns
            if not col.endswith("_label") and any(token in col.lower() for token in candidates)
        ]
        if not matching:
            continue

        column = matching[0]
        keep = filtered.select(pl.col(column).drop_nulls().unique().sort().head(limit)).to_series().to_list()
        filtered = filtered.filter(pl.col(column).is_in(keep))

    return filtered


def append_to_raw_parquet(frame: pl.DataFrame, target_path: Path) -> pl.DataFrame:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        existing = pl.read_parquet(target_path)
        combined = pl.concat([existing, frame], how="diagonal_relaxed")
    else:
        combined = frame

    combined.write_parquet(target_path)
    return combined


def load_dataset_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)
