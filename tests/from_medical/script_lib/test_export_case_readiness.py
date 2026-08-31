from __future__ import annotations

from scripts.from_medical.script_lib.export_case_readiness import refresh_export_case_readiness
from scripts.from_medical.script_lib.check_exam_results import fetch_target_case_ledgers
from scripts.from_medical.script_lib import check_exam_results


class RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.rowcount = 1

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))


def test_readiness_excludes_resolved_and_excluded_reviews_from_approval_aggregation() -> None:
    cur = RecordingCursor()

    refresh_export_case_readiness(cur, health_db="health_exam_result", event_id=2)

    approval_sql, approval_params = cur.calls[0]
    assert approval_params == (2,)
    assert approval_sql.count("NOT IN ('RESOLVED_BY_SOURCE_VALUE', 'EXCLUDED')") == 8
    assert approval_sql.count("cri.`review_status` = 'APPROVED_WITH_REASON'") == 3
    assert approval_sql.count("NULLIF(TRIM(cri.`review_note`), '') IS NULL") == 4
    assert "cri.`review_note`" in approval_sql


def test_readiness_can_update_one_case_without_touching_other_event_cases() -> None:
    cur = RecordingCursor()

    refresh_export_case_readiness(
        cur,
        health_db="health_exam_result",
        event_id=2,
        exam_export_case_id=3446,
    )

    assert len(cur.calls) == 2
    assert all(params == (2, 3446) for _sql, params in cur.calls)
    assert "eec.`exam_export_case_id` = %s" in cur.calls[0][0]
    assert "`exam_export_case_id` = %s" in cur.calls[1][0]


def test_fetch_target_case_ledgers_can_limit_by_case_ids() -> None:
    cur = RecordingCursor()
    cur.fetchall = lambda: []

    fetch_target_case_ledgers(
        cur,
        health_db="health_exam_result",
        event_id=2,
        case_ids=(3446, 3447),
    )

    sql, params = cur.calls[0]
    assert "exam_export_case_id IN (%s, %s)" in sql
    assert params == (2, 3446, 3447)


def test_recheck_auto_resolves_previously_approved_missing_item(monkeypatch) -> None:
    monkeypatch.setattr(check_exam_results, "parse_article44_missing_placeholder_items", lambda *_args: [])
    monkeypatch.setattr(check_exam_results, "specific_missing_placeholder_items_from_details", lambda *_args: [])
    monkeypatch.setattr(check_exam_results, "parse_specific_missing_placeholder_items", lambda *_args: [])

    class ReviewCursor(RecordingCursor):
        def fetchall(self) -> list[dict[str, object]]:
            return [
                {
                    "exam_case_check_review_item_id": 99,
                    "check_scope": "ARTICLE44",
                    "check_item_code": "4410001001",
                    "review_status": "APPROVED_WITH_REASON",
                }
            ]

    cur = ReviewCursor()
    changed = check_exam_results.sync_export_case_missing_placeholders(
        cur,
        health_db="health_exam_result",
        ledger={"ledger_type": "EXPORT_CASE", "id": 3446, "event_id": 2},
        article44_result={},
        article44_required_namecodes_by_detail={},
        specific_summary=None,
        specific_required_namecodes=(),
        specific_detail_results={},
    )

    assert changed == 1
    update_sql, update_params = cur.calls[1]
    assert "review_status = 'RESOLVED_BY_SOURCE_VALUE'" in update_sql
    assert "review_note = NULL" in update_sql
    assert "reviewed_by_app_user_id = NULL" in update_sql
    assert update_params == (99,)
    audit_sql, audit_params = cur.calls[2]
    assert "CHECK_EXAM_RESULTS" in audit_sql
    assert audit_params[5] == "APPROVED_WITH_REASON"
