from etl.config import load_config
from etl.ingest import (
    fetch_metadata,
    filter_metadata,
    run_raw_ingestion_pipeline,
    select_dry_run_scope,
)


def test_dry_run_scope_limits_records() -> None:
    cfg = load_config("config/settings.yaml")
    metadata = fetch_metadata(cfg)
    filtered = filter_metadata(metadata, cfg)
    scoped = select_dry_run_scope(filtered, cfg)

    assert len({item["municipality_code"] for item in scoped}) <= cfg.ingestion.dry_run_org_limit
    assert len({item["year"] for item in scoped}) <= cfg.ingestion.dry_run_year_limit


def test_pipeline_outputs_rows() -> None:
    cfg = load_config("config/settings.yaml")
    rows, schema = run_raw_ingestion_pipeline(cfg)

    assert len(rows) > 0
    assert "metric" in schema
    assert "value" in schema
