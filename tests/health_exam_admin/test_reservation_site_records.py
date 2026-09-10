from pathlib import Path

from apps.health_exam_admin.main import (
    load_reservation_site_month_options,
    load_reservation_site_record_list,
    reservation_status_label,
)


class ReservationListCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self._result_index = 0

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> dict[str, object]:
        return {
            "total_count": 101,
            "provisional_count": 1,
            "confirmed_count": 2,
            "visited_count": 3,
            "cancelled_count": 4,
            "unmapped_count": 5,
        }

    def fetchall(self) -> list[dict[str, object]]:
        return [{"reservation_id": 99, "reservation_status_raw": "3"}]


def test_reservation_site_record_list_filters_and_pages() -> None:
    cur = ReservationListCursor()
    result = load_reservation_site_record_list(
        cur,
        query_params={
            "event_id": "2",
            "status": "3",
            "exam_month": "2026-08,2026-09",
            "created_from": "2026-07-01",
            "created_to": "2026-07-31",
            "facility_link": "UNMAPPED",
            "option": "胃カメラ",
            "page": "2",
        },
    )

    assert result["page"] == 2
    assert result["page_count"] == 2
    assert result["rows"][0]["reservation_id"] == 99
    count_sql, count_params = cur.calls[0]
    rows_sql, rows_params = cur.calls[1]
    assert "r.event_id = %s" in count_sql
    assert "r.exam_facility_id IS NULL" in count_sql
    assert "reservation_site_record_options" in count_sql
    assert "DATE_FORMAT(r.reservation_date, '%Y-%m') IN (%s, %s)" in count_sql
    assert "r.source_created_at >= %s" in count_sql
    assert "r.source_created_at < DATE_ADD(%s, INTERVAL 1 DAY)" in count_sql
    assert "LIMIT %s OFFSET %s" in rows_sql
    assert count_params == (2, "3", "2026-08", "2026-09", "2026-07-01 00:00:00", "2026-07-31", "%胃カメラ%", "%胃カメラ%")
    assert rows_params[-2:] == (100, 100)


def test_reservation_status_labels() -> None:
    assert reservation_status_label("1") == "仮予約"
    assert reservation_status_label("3") == "予約確定"
    assert reservation_status_label("4") == "受診済み"
    assert reservation_status_label("5") == "キャンセル"


def test_reservation_month_options_use_event_and_reservation_counts() -> None:
    cur = ReservationListCursor()

    load_reservation_site_month_options(cur, event_id="2")

    sql, params = cur.calls[0]
    assert "event_id = %s" in sql
    assert "COUNT(*) AS reservation_count" in sql
    assert "cancelled_count" in sql
    assert params == (2, 36)


def test_reservation_site_record_page_has_search_and_list() -> None:
    template = Path("apps/health_exam_admin/templates/reservation_site_records.html").read_text(encoding="utf-8")
    assert "検索・絞り込み" in template
    assert "予約者・ID" in template
    assert "施設未紐付け" in template
    assert "予約一覧" in template
    assert "受診日（開始）" in template
    assert "予約登録日（開始）" in template
    assert "受診月" in template
    assert "data-month-picker" in template
    assert "予約 {{ option.reservation_count }}" in template
    assert "/utilities/reservation-site-csv" in template
