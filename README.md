# VK Data Platform

MVP backend for Finnish municipal finance analytics.

## Milestone 2 scope
- Raw ingestion pipeline for `/aineistot` metadata
- Reporting package whitelist filtering
- Dry-run dataset download scope (2 municipalities, 1 year)
- Long-format normalization
- Output to `data/facts_raw.parquet`

## Quickstart

```bash
python -m etl.cli --config config/settings.yaml --print-config
python -m etl.cli --config config/settings.yaml --run-pipeline
```

## Project structure

- `etl/` ingestion and pipeline code
- `api/` FastAPI service
- `config/` YAML configuration
- `data/` local data lake (raw + curated)
- `tests/` automated tests
