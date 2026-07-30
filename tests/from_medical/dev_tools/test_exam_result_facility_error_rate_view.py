from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DDL_PATH = (
    REPO_ROOT
    / "sql/ddl/health_exam_result/0130_health_exam_result__exam_result_facility_error_rate.sql"
)
MIGRATION_PATH = (
    REPO_ROOT
    / "sql/migrations/health_exam_result"
    / "20260730_010_health_exam_result_create_facility_error_rate_view.sql"
)


def test_view_ddl_and_migration_stay_identical() -> None:
    assert DDL_PATH.read_text(encoding="utf-8") == MIGRATION_PATH.read_text(encoding="utf-8")


def test_view_counts_people_before_calculating_error_rate() -> None:
    sql = DDL_PATH.read_text(encoding="utf-8")

    assert "SQL SECURITY INVOKER VIEW" in sql
    assert "CONCAT('SUBSCRIBER:', r.`subscriber_id`)" in sql
    assert "CONCAT('IDENTITY:', r.`identity_hash`)" in sql
    assert "SUM(person_rows.`has_ng`) AS `error_person_count`" in sql
    assert "NULLIF(COUNT(*), 0)" in sql
    assert "AS `error_rate_percent`" in sql
    assert "AS `checked_error_rate_percent`" in sql
