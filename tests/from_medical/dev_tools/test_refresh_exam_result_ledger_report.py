from __future__ import annotations

import re
import sys
from pathlib import Path

from scripts.from_medical.dev_tools import refresh_exam_result_ledger_report as report


REPO_ROOT = Path(__file__).resolve().parents[3]


def ddl_columns(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"^\s*`([a-z0-9_]+)`\s+", text, flags=re.MULTILINE))


def test_event_id_defaults_to_two(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["refresh_exam_result_ledger_report.py"])

    args = report.parse_args()

    assert args.event_id == 2


def test_source_mappings_cover_every_ledger_column() -> None:
    xml_columns = ddl_columns(
        REPO_ROOT / "sql/ddl/health_exam_result/0050_health_exam_result__xml_ledger.sql"
    )
    csv_columns = ddl_columns(
        REPO_ROOT / "sql/ddl/health_exam_result/0090_health_exam_result__csv_row_ledger.sql"
    )

    assert set(report.XML_SOURCE_TO_REPORT) == xml_columns
    assert set(report.CSV_SOURCE_TO_REPORT) == csv_columns


def test_report_projection_covers_every_report_table_column() -> None:
    report_columns = ddl_columns(
        REPO_ROOT
        / "sql/ddl/health_exam_result/0100_health_exam_result__exam_result_ledger_report.sql"
    )

    assert len(report.REPORT_COLUMNS) == len(set(report.REPORT_COLUMNS))
    assert set(report.REPORT_COLUMNS) | {"report_row_id"} == report_columns
    assert set(report.XML_SOURCE_TO_REPORT.values()) <= set(report.REPORT_COLUMNS)
    assert set(report.CSV_SOURCE_TO_REPORT.values()) <= set(report.REPORT_COLUMNS)


def test_xml_insert_adds_subscriber_fields_and_nulls_csv_only_fields() -> None:
    config = report.ReportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=True,
    )

    sql = report.build_insert_sql(config, "XML")

    assert "FROM `health_exam_result`.`xml_ledger` AS l" in sql
    assert "LEFT JOIN `dev_phr`.`subscribers` AS s" in sql
    assert "s.`relationship_name` AS `relationship_name`" in sql
    assert "s.`qualification_lost_date` AS `qualification_lost_date`" in sql
    assert "NULL AS `file_receipt_id`" in sql
    assert "l.`created_at` AS `source_created_at`" in sql
    assert sql.count("%s") == 2


def test_csv_insert_preserves_csv_specific_fields() -> None:
    config = report.ReportConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=True,
    )

    sql = report.build_insert_sql(config, "CSV")

    assert "FROM `health_exam_result`.`csv_row_ledger` AS l" in sql
    assert "l.`etl_run_id` AS `source_etl_run_id`" in sql
    assert "l.`raw_row_json` AS `raw_row_json`" in sql
    assert "l.`row_status` AS `row_status`" in sql
    assert "NULL AS `xml_sha256`" in sql
    assert sql.count("%s") == 2
