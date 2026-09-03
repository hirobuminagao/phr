from __future__ import annotations

from scripts.support_scripts.supersede_orphaned_exam_export_cases import classify_pair


def cases() -> tuple[dict[str, object], dict[str, object]]:
    old = {
        "event_id": 2,
        "subscriber_id": 10,
        "exam_date": "2026-05-01",
        "facility_code": "0110000001",
        "exam_facility_id": 1,
        "case_lifecycle_status": "ACTIVE",
        "active_source_count": 0,
        "value_count": 0,
        "xml_export_status": "ERROR",
        "created_at": 1,
    }
    successor = {
        **old,
        "exam_facility_id": 2,
        "active_source_count": 1,
        "value_count": 20,
        "xml_export_status": "PENDING",
        "created_at": 2,
    }
    return old, successor


def no_dependencies() -> dict[str, int]:
    return {
        "human_reviews": 0,
        "corrections": 0,
        "manual_drafts": 0,
        "export_members": 0,
        "active_export_list_entries": 1,
        "active_exported_list_entries": 0,
    }


def test_empty_old_case_is_eligible() -> None:
    old, successor = cases()
    assert classify_pair(old, successor, no_dependencies()) == []


def test_export_history_blocks_supersede() -> None:
    old, successor = cases()
    dependencies = no_dependencies()
    dependencies["export_members"] = 1
    assert "OLD_HAS_EXPORT_HISTORY" in classify_pair(old, successor, dependencies)


def test_human_review_blocks_supersede() -> None:
    old, successor = cases()
    dependencies = no_dependencies()
    dependencies["human_reviews"] = 1
    assert "OLD_HAS_HUMAN_REVIEW" in classify_pair(old, successor, dependencies)


def test_facility_must_have_changed() -> None:
    old, successor = cases()
    successor["exam_facility_id"] = old["exam_facility_id"]
    assert "FACILITY_ID_NOT_CHANGED" in classify_pair(old, successor, no_dependencies())
