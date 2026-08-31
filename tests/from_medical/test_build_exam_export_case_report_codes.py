from __future__ import annotations

import importlib


build_cases = importlib.import_module("scripts.from_medical.03_01_build_exam_export_cases")


def row(source_type: str, ledger_id: int, **values: object) -> dict[str, object]:
    return {"source_type": source_type, "exam_ledger_id": ledger_id, "check_status": "OK", **values}


def test_xml_explicit_pair_wins_over_csv() -> None:
    result = build_cases.resolve_report_codes(
        [
            row("CSV", 2, health_exam_report_category="40", program_code="990"),
            row("XML", 1, report_category_code="10", program_type_code="010"),
        ],
        event_year=2026,
    )
    assert (result.report_category, result.program_code) == ("10", "010")
    assert result.source == "XML_EXPLICIT"


def test_csv_pair_is_used_when_xml_has_no_codes() -> None:
    result = build_cases.resolve_report_codes(
        [row("XML", 1, birthdate="1980-01-01"), row("CSV", 2, health_exam_report_category="40", program_code="990")],
        event_year=2026,
    )
    assert (result.report_category, result.program_code) == ("40", "990")
    assert result.source == "CSV_EXPLICIT"


def test_known_xml_code_completes_its_pair_without_mixing_sources() -> None:
    result = build_cases.resolve_report_codes(
        [row("XML", 1, report_category_code="10"), row("CSV", 2, health_exam_report_category="40", program_code="990")],
        event_year=2026,
    )
    assert (result.report_category, result.program_code) == ("10", "010")
    assert result.source == "XML_KNOWN_PAIR"


def test_age_default_is_used_only_when_sources_have_no_usable_codes() -> None:
    result = build_cases.resolve_report_codes([row("XML", 1, birthdate="1986-04-01")], event_year=2026)
    assert (result.report_category, result.program_code) == ("10", "010")
    assert result.source == "EVENT_AGE_DEFAULT"


def test_missing_birthdate_keeps_codes_unresolved() -> None:
    result = build_cases.resolve_report_codes([row("XML", 1)], event_year=2026)
    assert result.report_category is None
    assert result.program_code is None
    assert "BIRTHDATE_MISSING" in str(result.reason)


class CorrectionCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return [
            {
                "field_code": "report_codes",
                "normalized_value": "40|990",
                "correction_reason": "健診機関へ確認済み",
            }
        ]


def test_report_code_manual_correction_is_reapplied_after_case_rebuild() -> None:
    cursor = CorrectionCursor()
    config = build_cases.BuildCaseConfig(
        event_id=2,
        health_db="health_exam_result",
        dev_db="dev_phr",
        dry_run=False,
        limit_groups=0,
    )

    build_cases.reapply_basic_info_corrections(cursor, config, case_id=51072)

    update_sql, update_params = cursor.calls[1]
    assert "`health_exam_report_category` = %s" in update_sql
    assert "`program_code` = %s" in update_sql
    assert "`report_code_resolution_source` = 'MANUAL_CORRECTION'" in update_sql
    assert update_params == ("40", "990", "健診機関へ確認済み", 51072)
