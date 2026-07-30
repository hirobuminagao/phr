from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "from_medical" / "04_export_hia_xml.py"
SPEC = importlib.util.spec_from_file_location("export_hia_xml", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
export_hia_xml = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = export_hia_xml
SPEC.loader.exec_module(export_hia_xml)


def args_for(config: Path, **overrides: object) -> argparse.Namespace:
    data = {
        "config": str(config),
        "event_id": None,
        "facility_id": [],
        "facility_code": [],
        "all_facilities": False,
        "file_receipt_id": [],
        "ledger_id": [],
        "exam_month": None,
        "include_exported": False,
        "split_no": None,
        "file_date": None,
        "limit": None,
        "dry_run": False,
        "db_prefix": "PHR_DB_",
        "health_db": None,
        "dev_db": None,
        "master_db": None,
    }
    data.update(overrides)
    return argparse.Namespace(**data)


def write_config(path: Path, **values: object) -> Path:
    path.write_text(yaml.safe_dump(values, allow_unicode=True), encoding="utf-8")
    return path


def test_load_config_accepts_yaml_selectors(tmp_path: Path) -> None:
    config_path = write_config(
        tmp_path / "export.yml",
        event_id=2,
        facility_ids=[10, 20],
        facility_codes=["0123456789", "9876543210"],
        file_receipt_ids=[101],
        ledger_ids=[1001, 1002],
        exam_month="2026-06",
        split_no=3,
        file_date="20260730",
        dry_run=True,
    )

    config = export_hia_xml.load_config(args_for(config_path))

    assert config.selectors.event_id == 2
    assert config.selectors.facility_ids == (10, 20)
    assert config.selectors.facility_codes == ("0123456789", "9876543210")
    assert config.selectors.file_receipt_ids == (101,)
    assert config.selectors.ledger_ids == (1001, 1002)
    assert config.selectors.exam_month == "2026-06"
    assert config.split_no == 3
    assert config.file_date.isoformat() == "2026-07-30"
    assert config.dry_run is True


def test_cli_selectors_override_yaml_lists(tmp_path: Path) -> None:
    config_path = write_config(
        tmp_path / "export.yml",
        event_id=2,
        facility_ids=[10],
        facility_codes=["0123456789"],
        file_receipt_ids=[101],
    )

    config = export_hia_xml.load_config(
        args_for(config_path, facility_id=[30], facility_code=["1111111111"], file_receipt_id=[202])
    )

    assert config.selectors.facility_ids == (30,)
    assert config.selectors.facility_codes == ("1111111111",)
    assert config.selectors.file_receipt_ids == (202,)


def test_requires_explicit_scope(tmp_path: Path) -> None:
    config_path = write_config(tmp_path / "export.yml", event_id=2, exam_month="2026-06")

    with pytest.raises(ValueError, match="Specify --facility-id"):
        export_hia_xml.load_config(args_for(config_path))
