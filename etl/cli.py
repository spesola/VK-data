from __future__ import annotations

import argparse
import json

from etl.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="VK Data ETL CLI")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to YAML config")
    parser.add_argument("--print-config", action="store_true", help="Print resolved configuration")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.print_config:
        print(json.dumps(cfg.model_dump(mode="json"), indent=2, ensure_ascii=False))
        return

    print("Scaffold ready. Next milestone: metadata fetch + schema inspection.")


if __name__ == "__main__":
    main()
