from apps.health_exam_admin.main import (
    build_approved_with_reason_csv,
    load_approved_with_reason_rows,
)


class Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = " ".join(sql.split())
        self.params = params

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


def test_approved_with_reason_query_ignores_case_output_state_unless_filtered() -> None:
    cur = Cursor([])

    load_approved_with_reason_rows(
        cur,
        filters={"event_id": "2", "check_scope": "ARTICLE44"},
    )

    assert "cri.review_status = 'APPROVED_WITH_REASON'" in cur.sql
    assert "eec.export_readiness_status = %s" not in cur.sql
    assert cur.params == (2, "ARTICLE44", 5000)


def test_approved_with_reason_query_can_filter_current_output_state() -> None:
    cur = Cursor([])

    load_approved_with_reason_rows(
        cur,
        filters={"export_readiness_status": "EXPORTED"},
    )

    assert "eec.export_readiness_status = %s" in cur.sql
    assert cur.params == ("EXPORTED", 5000)


def test_approved_with_reason_query_supports_person_and_facility_filters() -> None:
    cur = Cursor([])

    load_approved_with_reason_rows(
        cur,
        filters={
            "insurance_symbol": "AB",
            "insurance_number": "123",
            "name_kana": "ヤマダ",
            "name_full": "山田",
            "qualification_lost_status": "LOST",
            "qualification_lost_date": "2026-08-31",
            "hia_subscriber_id": "HIA",
            "subscriber_id": "10",
            "case_id": "20",
            "exam_facility_id": "30",
        },
    )

    assert "s.qualification_lost_date IS NOT NULL" in cur.sql
    assert "eec.exam_facility_id = %s" in cur.sql
    assert "eec.insurance_symbol_export_value LIKE %s" in cur.sql
    assert "CAST(eec.subscriber_id AS CHAR) = %s" in cur.sql
    assert cur.params[-4:] == ("10", "30", "2026-08-31", 5000)


def test_approved_with_reason_csv_keeps_one_row_per_case_with_split_reasons() -> None:
    content = build_approved_with_reason_csv(
        [
            {
                "event_id": 2,
                "exam_export_case_id": 10,
                "approved_item_count": 2,
                "legal_reason_count": 1,
                "legal_reasons": "視力 [4403004001]：医師判断により実施省略",
                "specific_reason_count": 1,
                "specific_reasons": "尿糖 [4410001001]：妊娠中のため未実施",
                "export_readiness_status": "EXPORTED",
            }
        ]
    )

    assert "法定の理由" in content
    assert "特定健診の理由" in content
    assert "医師判断により実施省略" in content
    assert "妊娠中のため未実施" in content
    assert "EXPORTED" in content


def test_approved_with_reason_query_groups_review_items_by_case() -> None:
    cur = Cursor([])

    load_approved_with_reason_rows(cur, filters={})

    assert "GROUP BY cri.exam_export_case_id" in cur.sql
    assert "AS legal_reasons" in cur.sql
    assert "AS specific_reasons" in cur.sql
