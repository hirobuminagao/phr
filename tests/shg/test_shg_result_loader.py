from scripts.shg.script_lib.shg_result_loader import (
    SHG_RESULT_SELECT_SQL,
    build_shg_result_candidate_map,
    build_latest_shg_result_map,
    select_shg_result_candidate,
)


def test_latest_shg_year_is_selected_for_same_identity_hash() -> None:
    rows = [
        {"id": 30, "identity_hash": "person-a", "shg_year": 2025, "exam_waist_cm": 80},
        {"id": 20, "identity_hash": "person-a", "shg_year": 2024, "exam_waist_cm": 90},
        {"id": 10, "identity_hash": "person-b", "shg_year": 2023, "exam_waist_cm": 70},
    ]

    result = build_latest_shg_result_map(rows)

    assert result["person-a"]["id"] == 30
    assert result["person-a"]["shg_year"] == 2025
    assert result["person-a"]["exam_waist_cm"] == 80
    assert result["person-b"]["id"] == 10


def test_same_year_uses_newest_id_and_empty_identity_is_ignored() -> None:
    rows = [
        {"id": 31, "identity_hash": "person-a", "shg_year": 2025, "exam_weight_kg": 60},
        {"id": 30, "identity_hash": "person-a", "shg_year": 2025, "exam_weight_kg": 65},
        {"id": 40, "identity_hash": "", "shg_year": 2026, "exam_weight_kg": 70},
    ]

    result = build_latest_shg_result_map(rows)

    assert result["person-a"]["id"] == 31
    assert result["person-a"]["exam_weight_kg"] == 60
    assert len(result) == 1


def test_query_orders_candidates_from_newest_to_oldest() -> None:
    assert "ORDER BY identity_hash, shg_year DESC, id DESC" in SHG_RESULT_SELECT_SQL


def test_xml_health_checkup_date_selects_matching_older_year() -> None:
    candidates = build_shg_result_candidate_map(
        [
            {"id": 30, "identity_hash": "person-a", "shg_year": 2025, "health_checkup_date": "2025-06-01"},
            {"id": 20, "identity_hash": "person-a", "shg_year": 2024, "health_checkup_date": "2024-05-20"},
        ]
    )["person-a"]

    selected = select_shg_result_candidate(candidates, xml_health_checkup_date="20240520")

    assert selected["id"] == 20


def test_missing_date_match_falls_back_to_latest_year() -> None:
    candidates = build_shg_result_candidate_map(
        [
            {"id": 30, "identity_hash": "person-a", "shg_year": 2025, "health_checkup_date": "2025-06-01"},
            {"id": 20, "identity_hash": "person-a", "shg_year": 2024, "health_checkup_date": "2024-05-20"},
        ]
    )["person-a"]

    selected = select_shg_result_candidate(candidates, xml_health_checkup_date="20230101")

    assert selected["id"] == 30
