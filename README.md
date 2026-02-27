# VK Data Platform

MVP backend for Finnish municipal finance analytics.

## Milestone 1 scope
- Python project scaffold with `uv`
- Folder layout for ETL/API/config/data/tests
- YAML-driven configuration system with environment overrides

## Quickstart

```bash
uv sync
uv run python -m etl.cli --config config/settings.yaml --print-config
uv run uvicorn api.main:app --reload
```

## Project structure

- `etl/` ingestion and pipeline code
- `api/` FastAPI service
- `config/` YAML configuration
- `data/` local data lake (raw + curated)
- `tests/` automated tests
