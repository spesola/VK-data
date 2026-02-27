from __future__ import annotations

import argparse
import json
from pathlib import Path

from etl.config import load_config
from etl.raw_storage import (
    append_to_raw_parquet,
    apply_dry_run_limits,
    load_dataset_json,
    normalize_dataset_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="VK Data ETL CLI")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to YAML config")
    parser.add_argument("--print-config", action="store_true", help="Print resolved configuration")
    parser.add_argument("--dataset-json", help="Path to one dataset JSON payload")
    parser.add_argument("--dry-run", action="store_true", help="Use limited dry-run data slice")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.print_config:
        print(json.dumps(cfg.model_dump(mode="json"), indent=2, ensure_ascii=False))
        return

    if not args.dataset_json:
        raise SystemExit("Missing --dataset-json. Provide a single dataset JSON input file.")

    dataset_payload = load_dataset_json(args.dataset_json)
    normalized = normalize_dataset_json(dataset_payload)

    if args.dry_run:
        normalized = apply_dry_run_limits(
            normalized,
            reporting_package_limit=1,
            organization_limit=cfg.ingestion.dry_run_org_limit,
            year_limit=cfg.ingestion.dry_run_year_limit,
        )

    target = Path(cfg.storage.base_path) / "facts_raw.parquet"
    combined = append_to_raw_parquet(normalized, target)

    print(f"facts_raw row count: {combined.height}")
    print("facts_raw schema:")
    for name, dtype in combined.schema.items():
        print(f"- {name}: {dtype}")


if __name__ == "__main__":
    main()
