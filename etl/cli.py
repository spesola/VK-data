from __future__ import annotations

import argparse
import json

from etl.config import load_config
from etl.ingest import run_raw_ingestion_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="VK Data ETL CLI")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to YAML config")
    parser.add_argument("--print-config", action="store_true", help="Print resolved configuration")
    parser.add_argument("--run-pipeline", action="store_true", help="Run raw ingestion pipeline")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.print_config:
        print(json.dumps(cfg.__dict__, default=str, indent=2, ensure_ascii=False))
        return

    if args.run_pipeline:
        rows, schema = run_raw_ingestion_pipeline(cfg)
        print(f"Wrote data/facts_raw.parquet")
        print(f"Schema: {schema}")
        print(f"Row count: {len(rows)}")
        return

    print("Use --run-pipeline to execute raw ingestion milestone pipeline.")


if __name__ == "__main__":
    main()
