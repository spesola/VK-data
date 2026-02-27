from etl.config import load_config


def test_load_config_defaults() -> None:
    cfg = load_config("config/settings.yaml")
    assert cfg.api.metadata_url.startswith("https://")
    assert "kunta" in cfg.ingestion.organization_types
    assert cfg.storage.raw_path.exists()
