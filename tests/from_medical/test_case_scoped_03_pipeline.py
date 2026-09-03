from __future__ import annotations

import importlib

from scripts.from_medical.script_lib.case_insurer_resolution import canonical_insurer_number


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


class SourceOwnerCursor(UpdateCursor):
    def __init__(self, owner_case_id: int) -> None:
        super().__init__()
        self.owner_case_id = owner_case_id

    def fetchone(self) -> dict[str, object]:
        return {"exam_export_case_id": self.owner_case_id}


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
    assert "manual_exam_export_case_id = %s" in source_sql
    assert "resolved_insurer_number IN" not in source_sql
    assert source_params == (2, 3446, 100, "2026-06-01", 70)


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


def test_case_grouping_does_not_split_on_insurer_number() -> None:
    rows = [
        {"event_id": 2, "subscriber_id": 100, "exam_date": "2026-06-01", "exam_facility_id": 70, "insurer_number": "00000000"},
        {"event_id": 2, "subscriber_id": 100, "exam_date": "2026-06-01", "exam_facility_id": 70, "insurer_number": "06139463"},
    ]

    groups = build_cases.select_groups(rows, 0)

    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_event_insurer_number_is_canonical_eight_digits() -> None:
    assert canonical_insurer_number("6139463") == "06139463"


def test_upsert_case_reuses_existing_case_when_only_insurer_differs() -> None:
    cur = SequentialCursor([[{"exam_export_case_id": 3446, "insurer_number": "00000000"}]])
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
    )
    params = {column: None for column in build_cases.CASE_COLUMNS}
    params.update(
        event_id=2,
        subscriber_id=100,
        exam_date="2026-06-01",
        exam_facility_id=70,
        insurer_number="06139463",
    )

    case_id, action = build_cases.upsert_case(cur, config, params)

    assert (case_id, action) == (3446, "updated")
    assert "WHERE `exam_export_case_id` = %s" in cur.calls[1][0]
    assert cur.calls[1][1][-1] == 3446
    assert cur.calls[1][1][build_cases.CASE_COLUMNS.index("insurer_number") - 1] == "06139463"


def test_upsert_case_rejects_ambiguous_existing_cases() -> None:
    cur = SequentialCursor(
        [[
            {"exam_export_case_id": 3446, "insurer_number": "00000000"},
            {"exam_export_case_id": 51072, "insurer_number": "06139463"},
        ]]
    )
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
    )
    params = {column: None for column in build_cases.CASE_COLUMNS}
    params.update(
        event_id=2,
        subscriber_id=100,
        exam_date="2026-06-01",
        exam_facility_id=70,
        insurer_number="06139463",
    )

    try:
        build_cases.upsert_case(cur, config, params)
    except RuntimeError as exc:
        assert str(exc) == "DUPLICATE_CASE_IDENTITY: case_ids=3446,51072"
    else:
        raise AssertionError("duplicate cases must not be merged implicitly")


def test_upsert_sources_does_not_move_ledger_from_another_case() -> None:
    cur = SourceOwnerCursor(owner_case_id=123073)
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
    )
    ledger = {
        "exam_ledger_id": 173,
        "source_type": "XML",
        "file_receipt_id": 10,
    }

    try:
        build_cases.upsert_sources(cur, config, case_id=3446, group=[ledger], primary=ledger)
    except RuntimeError as exc:
        assert str(exc) == (
            "CASE_SOURCE_OWNERSHIP_CONFLICT: "
            "ledger_id=173 owner_case_id=123073 requested_case_id=3446"
        )
    else:
        raise AssertionError("normal case rebuild must not transfer source ownership")
    assert len(cur.calls) == 1


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
