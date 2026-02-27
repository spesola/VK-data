from __future__ import annotations

from pathlib import Path

import polars as pl

from etl.raw_storage import append_to_raw_parquet, apply_dry_run_limits, normalize_dataset_json


def _sample_dataset() -> dict:
    return {
        "dataset": {
            "id": ["Raportointikokonaisuus", "Organisaatio", "Vuosi"],
            "dimension": {
                "Raportointikokonaisuus": {
                    "category": {
                        "index": {"Käyttötalous": 0, "Tuloslaskelma": 1},
                        "label": {
                            "Käyttötalous": "Käyttötalous",
                            "Tuloslaskelma": "Tuloslaskelma",
                        },
                    }
                },
                "Organisaatio": {
                    "category": {
                        "index": {"091": 0, "049": 1, "235": 2},
                        "label": {"091": "Helsinki", "049": "Espoo", "235": "Kauniainen"},
                    }
                },
                "Vuosi": {
                    "category": {
                        "index": {"2022": 0, "2023": 1},
                        "label": {"2022": "2022", "2023": "2023"},
                    }
                },
            },
            "value": list(range(12)),
        }
    }


def test_normalize_dataset_json_long_form() -> None:
    frame = normalize_dataset_json(_sample_dataset())

    assert frame.height == 12
    assert "raportointikokonaisuus" in frame.columns
    assert "organisaatio" in frame.columns
    assert "vuosi" in frame.columns
    assert frame.select("value").to_series().max() == 11


def test_dry_run_limits_and_append(tmp_path: Path) -> None:
    frame = normalize_dataset_json(_sample_dataset())
    dry_run = apply_dry_run_limits(frame, reporting_package_limit=1, organization_limit=2, year_limit=1)

    assert dry_run["raportointikokonaisuus"].n_unique() == 1
    assert dry_run["organisaatio"].n_unique() == 2
    assert dry_run["vuosi"].n_unique() == 1

    target = tmp_path / "facts_raw.parquet"
    first_write = append_to_raw_parquet(dry_run, target)
    second_write = append_to_raw_parquet(dry_run, target)

    assert target.exists()
    assert isinstance(pl.read_parquet(target), pl.DataFrame)
    assert second_write.height == first_write.height * 2
