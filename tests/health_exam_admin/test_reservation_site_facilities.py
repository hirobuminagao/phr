from pathlib import Path

from apps.health_exam_admin.main import (
    facility_name_match_score,
    load_reservation_facility_candidates,
    normalize_facility_name_for_match,
    update_reservation_site_facility_mapping,
)


class CandidateCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        if len(self.calls) == 1:
            return [{"exam_facility_id": 2, "case_match_people": 8, "case_evidence_total": 10}]
        return [
            {"exam_facility_id": 2, "exam_facility_name": "札幌中央病院", "exam_facility_display_name": None},
            {"exam_facility_id": 1, "exam_facility_name": "医療法人社団 札幌中央病院", "exam_facility_display_name": None},
        ]


def test_facility_name_match_normalizes_legal_entity_and_width() -> None:
    assert normalize_facility_name_for_match("医療法人社団　札幌中央病院") == "札幌中央病院"
    assert facility_name_match_score("医療法人社団　札幌中央病院", "札幌中央病院") == 100


def test_candidates_are_sorted_by_name_score() -> None:
    cur = CandidateCursor()
    rows = load_reservation_facility_candidates(cur, hospital_id=77, hospital_name="札幌中央病院")

    assert rows[0]["exam_facility_id"] == 2
    assert rows[0]["case_match_rate"] == 80
    assert rows[0]["case_match_people"] == 8
    assert "person_event" in cur.calls[0][0]
    assert "exam_export_cases" in cur.calls[0][0]
    assert "HAVING COUNT(DISTINCT subscriber_id)=1" in cur.calls[0][0]
    assert cur.calls[0][1] == (77,)
    assert "exam_facilities" in cur.calls[1][0]
    assert "is_active=1" in cur.calls[1][0]


def test_facility_mapping_page_has_status_candidates_and_registration() -> None:
    template = Path("apps/health_exam_admin/templates/reservation_site_facilities.html").read_text(encoding="utf-8")

    assert "予約施設ID一覧" in template
    assert "未紐付け" in template
    assert "一致率" in template
    assert "case実績" in template
    assert "この機関に紐付け" in template
    assert "_csrf_token" in template


def test_mapping_update_refreshes_existing_reservation_rows() -> None:
    source = update_reservation_site_facility_mapping.__wrapped__ if hasattr(update_reservation_site_facility_mapping, "__wrapped__") else update_reservation_site_facility_mapping
    text = __import__("inspect").getsource(source)

    assert "reservation_site_facility_mappings" in text
    assert "ON DUPLICATE KEY UPDATE" in text
    assert "UPDATE {qname(work_other_db())}.reservation_site_records" in text
