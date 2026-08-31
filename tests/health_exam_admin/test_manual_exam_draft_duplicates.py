from apps.health_exam_admin.main import (
    annotate_manual_exam_draft_duplicates,
    filter_manual_exam_entry_draft_rows,
    summarize_manual_exam_entry_drafts,
)


def draft(
    draft_id: int,
    *,
    event_id: int = 2,
    subscriber_id: int | None = None,
    hia_id: str = "",
    case_id: int | None = None,
    status: str = "DRAFT",
) -> dict[str, object]:
    return {
        "manual_exam_entry_draft_id": draft_id,
        "event_id": event_id,
        "subscriber_id": subscriber_id,
        "hia_subscriber_id": hia_id,
        "person_id_custom": "",
        "insurer_number": "",
        "insurance_symbol": "",
        "insurance_number": "",
        "insurance_branch_number": "",
        "name_kana": "",
        "name_full": "",
        "birthdate": None,
        "exam_export_case_id": case_id,
        "draft_status": status,
    }


def test_duplicate_candidates_are_grouped_within_the_same_event() -> None:
    rows = [
        draft(1, subscriber_id=10, hia_id="HIA-001", case_id=101),
        draft(2, subscriber_id=10, hia_id="HIA-001", case_id=102, status="READY"),
        draft(3, event_id=3, subscriber_id=10, hia_id="HIA-001", case_id=103),
    ]

    annotate_manual_exam_draft_duplicates(rows)

    assert rows[0]["draft_duplicate_count"] == 2
    assert rows[1]["draft_duplicate_count"] == 2
    assert rows[2]["draft_duplicate_count"] == 1
    assert rows[0]["draft_duplicate_refs"] == [
        {"manual_exam_entry_draft_id": 2, "draft_status": "READY", "name": "氏名未設定"}
    ]
    summary = summarize_manual_exam_entry_drafts(rows)
    assert summary["duplicate"] == 2
    assert summary["duplicate_groups"] == 1


def test_hia_case_and_duplicate_filters_can_be_combined() -> None:
    rows = [
        draft(1, hia_id="HIA-ABC-001", case_id=1201),
        draft(2, hia_id="HIA-ABC-001", case_id=1202),
        draft(3, hia_id="HIA-XYZ-009", case_id=9901),
    ]
    annotate_manual_exam_draft_duplicates(rows)

    filtered = filter_manual_exam_entry_draft_rows(
        rows,
        status_filter="",
        hia_subscriber_id="abc",
        case_id="1202",
        duplicates_only=True,
    )

    assert [row["manual_exam_entry_draft_id"] for row in filtered] == [2]
