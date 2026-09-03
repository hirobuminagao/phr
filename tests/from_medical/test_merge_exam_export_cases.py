from __future__ import annotations

from scripts.support_scripts.merge_exam_export_cases import review_is_automatic


def automatic_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "source_status": "NEEDS_CONFIRMATION",
        "source_note": None,
        "source_reviewed_by": None,
        "source_human_audit_count": 0,
        "target_status": "RESOLVED_BY_SOURCE_VALUE",
        "target_note": None,
        "target_reviewed_by": None,
        "target_human_audit_count": 0,
    }
    row.update(overrides)
    return row


def test_automatic_missing_and_resolved_review_can_be_absorbed() -> None:
    assert review_is_automatic(automatic_row()) is True


def test_approved_with_reason_still_blocks_merge() -> None:
    assert review_is_automatic(automatic_row(source_status="APPROVED_WITH_REASON")) is False


def test_review_note_still_blocks_merge() -> None:
    assert review_is_automatic(automatic_row(target_note="確認済み")) is False


def test_human_audit_still_blocks_merge() -> None:
    assert review_is_automatic(automatic_row(source_human_audit_count=1)) is False
