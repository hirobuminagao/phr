from __future__ import annotations

from scripts.from_medical.script_lib.hia_xml_export_loader import (
    ExportSelectors,
    check_reason_is_missing_only,
    decide_candidate,
    detect_unresolved_duplicates,
    facility_folder_name,
    fetch_candidates,
    fetch_valid_items,
)


def base_row() -> dict:
    return {
        "csv_row_ledger_id": 1,
        "event_id": 2,
        "exam_facility_id": 10,
        "insurer_number": "06139463",
        "subscriber_id": 100,
        "exam_date": "2026-06-01",
        "health_exam_report_category": "10",
        "program_code": "010",
        "address": "東京都千代田区",
        "subscriber_match_status": "MATCHED",
        "export_readiness_status": "EXPORT_READY",
        "check_status": "OK",
        "check_reason": None,
        "manual_export_approved": 0,
    }


def test_manual_approval_allows_only_missing_check_results() -> None:
    row = base_row()
    row.update(
        check_status="NG",
        check_reason="4401001001:既往歴:MISSING | 4403005001:聴力:MISSING",
        manual_export_approved=1,
        manual_export_reason="妊娠中のため",
        manual_export_approved_at="2026-07-30 12:00:00",
        manual_export_approved_by="operator",
    )
    assert check_reason_is_missing_only(row["check_reason"])
    assert decide_candidate(row).allowed

    row["check_reason"] += " | 4403004001:視力:PARSE_ERROR"
    assert not decide_candidate(row).allowed


def test_case_approved_with_reason_is_exportable_after_excluded_review_is_ignored() -> None:
    row = base_row()
    row.update(
        export_readiness_status="APPROVED_WITH_REASON",
        check_status="NG",
        check_reason="4410001001:尿糖:MISSING | 4410001002:尿蛋白:MISSING",
        manual_export_approved=1,
        manual_export_reason="4410001001:尿糖:健診機関確認済み | 4410001002:尿蛋白:未実施を確認済み",
        manual_export_approved_at="2026-08-31 10:00:00",
        manual_export_approved_by="12",
    )

    assert decide_candidate(row).allowed


def test_duplicate_key_and_windows_facility_folder() -> None:
    first = base_row()
    second = {**first, "csv_row_ledger_id": 2, "insurer_number": "6139463"}
    assert detect_unresolved_duplicates([first, second]) == {1, 2}
    assert facility_folder_name(r"0110217718_テスト\02_健診結果\result.csv") == "0110217718_テスト"


def test_incomplete_candidate_is_not_treated_as_duplicate() -> None:
    first = {**base_row(), "subscriber_id": None}
    second = {**first, "csv_row_ledger_id": 2}
    assert detect_unresolved_duplicates([first, second]) == set()


def test_candidate_allows_missing_postal_code_when_address_exists() -> None:
    row = base_row()
    row.update(postal_code=None, postal_code_completed_value=None, address="東京都千代田区", address_completed_value=None)
    assert decide_candidate(row).allowed


def test_candidate_blocks_when_address_is_missing() -> None:
    row = base_row()
    row.update(postal_code=None, postal_code_completed_value=None, address=None, address_completed_value=None)
    decision = decide_candidate(row)
    assert not decision.allowed
    assert decision.reason == "ADDRESS_MISSING"


def test_fetch_valid_items_passes_annex2_and_source_metadata() -> None:
    class Cursor:
        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.sql = sql
            self.params = params

        def fetchall(self) -> list[dict]:
            return [
                {
                    "namecode": "2A040000001930102",
                    "section_code": "01010",
                    "value_type": "PQ",
                    "normalized_value": "34.6",
                    "normalized_unit": "%",
                    "nullflavor": None,
                    "code_system": None,
                    "code_value": None,
                    "code_display": None,
                    "interpretation_code": "L",
                    "interpretation_code_system": None,
                    "interpretation_name": None,
                    "display_name": "ヘマトクリット値",
                    "method_code": None,
                    "source_reference_lower": "35.5",
                    "source_reference_upper": "48.9",
                    "series_group_identifier": "2A020161001930149",
                    "series_group_relation_code": "COMP",
                    "negation_ind": None,
                    "occurrence_no": 1,
                    "jun_no": 840,
                }
            ]

    cur = Cursor()
    item = fetch_valid_items(cur, ledger_id=1, health_db="health_exam_result", dev_db="dev_phr", master_db="phr_master")[0]

    assert item.interpretation_code == "L"
    assert item.source_reference_lower == "35.5"
    assert item.source_reference_upper == "48.9"
    assert item.series_group_identifier == "2A020161001930149"
    assert item.series_group_relation_code == "COMP"
    assert "annex2_series_group_identifier" in cur.sql


def test_fetch_candidates_can_filter_by_facility_code() -> None:
    class Cursor:
        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.sql = sql
            self.params = params

        def fetchall(self) -> list[dict]:
            return []

    cur = Cursor()
    fetch_candidates(
        cur,
        selectors=ExportSelectors(event_id=2, facility_codes=("0123456789", "9876543210")),
        health_db="health_exam_result",
        master_db="phr_master",
    )

    assert "ef.exam_facility_code IN" in cur.sql
    assert cur.params == (2, "0123456789", "9876543210")
