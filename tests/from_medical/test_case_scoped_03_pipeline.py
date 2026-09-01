from __future__ import annotations

import importlib


build_cases = importlib.import_module("scripts.from_medical.03_01_build_exam_export_cases")
build_values = importlib.import_module("scripts.from_medical.03_02_build_exam_export_case_values")


class SequentialCursor:
    def __init__(self, results: list[list[dict[str, object]]]) -> None:
        self.results = results
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self.results.pop(0)


class UpdateCursor:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))


def test_build_cases_filters_sources_by_existing_case_key() -> None:
    cur = SequentialCursor(
        [
            [
                {
                    "exam_export_case_id": 3446,
                    "event_id": 2,
                    "subscriber_id": 100,
                    "exam_date": "2026-06-01",
                    "exam_facility_id": 70,
                    "insurer_number": "00000000",
                    "insurer_number_export_value": "06139463",
                }
            ],
            [],
        ]
    )
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
        case_ids=(3446,),
    )

    rows = build_cases.fetch_source_ledgers(cur, config)

    assert rows == []
    assert "exam_export_case_id IN (%s)" in cur.calls[0][0]
    assert cur.calls[0][1] == (2, 3446)
    source_sql, source_params = cur.calls[1]
    assert "resolved_subscriber_id = %s" in source_sql
    assert "resolved_exam_date = %s" in source_sql
    assert "NULLIF(el.`insurer_number_export_value`, '')" in source_sql
    assert "el.`insurer_number` REGEXP '^0+$'" in source_sql
    assert "eec.`insurer_number_export_value`" in source_sql
    assert "manual_exam_export_case_id = %s" in source_sql
    assert source_params == (2, 3446, 100, "2026-06-01", 70, "00000000", "06139463")


def test_build_cases_keeps_existing_case_key_after_insurer_completion() -> None:
    cur = SequentialCursor(
        [
            [
                {
                    "exam_export_case_id": 3446,
                    "event_id": 2,
                    "subscriber_id": 100,
                    "exam_date": "2026-06-01",
                    "exam_facility_id": 70,
                    "insurer_number": "00000000",
                    "insurer_number_export_value": "06139463",
                }
            ],
            [
                {
                    "event_id": 2,
                    "subscriber_id": 100,
                    "resolved_subscriber_id": 100,
                    "exam_date": "2026-06-01",
                    "resolved_exam_date": "2026-06-01",
                    "exam_facility_id": 70,
                    "resolved_exam_facility_id": 70,
                    "insurer_number": "00000000",
                    "resolved_insurer_number": "06139463",
                    "manual_exam_export_case_id": 3446,
                }
            ],
        ]
    )
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
        case_ids=(3446,),
    )

    rows = build_cases.fetch_source_ledgers(cur, config)

    assert rows[0]["resolved_insurer_number"] == "06139463"
    assert rows[0]["insurer_number"] == "00000000"


def test_reopen_export_error_only_resets_failed_case() -> None:
    cur = UpdateCursor()
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
        case_ids=(3446,),
    )

    updated = build_cases.reopen_export_error(cur, config, case_id=3446)

    assert updated == 1
    sql, params = cur.calls[0]
    assert "`xml_export_status` = 'PENDING'" in sql
    assert "`xml_export_status` = 'ERROR'" in sql
    assert "`xml_export_etl_run_id` = NULL" in sql
    assert params == (3446,)


def test_build_cases_can_target_subscriber_before_case_exists() -> None:
    cur = SequentialCursor([[]])
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
        subscriber_ids=(100,),
    )

    rows = build_cases.fetch_source_ledgers(cur, config)

    assert rows == []
    sql, params = cur.calls[0]
    assert "resolved_subscriber_id IN (%s)" in sql
    assert params == (2, 100)


def test_build_values_filters_cases_by_case_id() -> None:
    cur = SequentialCursor([[]])
    config = build_values.BuildValueConfig(
        event_id=2,
        health_db="health_exam_result",
        master_db="phr_master",
        dry_run=False,
        limit_cases=0,
        include_review_required=False,
        case_ids=(3446, 3447),
    )

    rows = build_values.fetch_cases(cur, config)

    assert rows == []
    sql, params = cur.calls[0]
    assert "`exam_export_case_id` IN (%s, %s)" in sql
    assert params == (2, 3446, 3447)
