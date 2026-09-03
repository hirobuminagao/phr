from inspect import getsource

from apps.health_exam_admin.main import (
    load_subscriber_match_candidate_rows,
    load_subscriber_match_resolution_counts,
    load_subscriber_match_resolved_rows,
    subscriber_match_issue_where_parts,
)


class Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchone(self) -> dict[str, object]:
        return {"resolved_person_count": 12, "resolved_ledger_count": 15}

    def fetchall(self) -> list[dict[str, object]]:
        return []


def test_load_subscriber_match_resolution_counts_uses_manual_unique_people() -> None:
    cursor = Cursor()

    result = load_subscriber_match_resolution_counts(cursor, event_id=2)

    assert result == {"resolved_person_count": 12, "resolved_ledger_count": 15}
    assert "COUNT(DISTINCT new_subscriber_id)" in cursor.sql
    assert "COUNT(DISTINCT exam_ledger_id)" in cursor.sql
    assert "new_subscriber_match_method = 'manual'" in cursor.sql
    assert cursor.params == (2,)


def test_default_subscriber_match_issues_exclude_confirmed_manual_ledgers() -> None:
    where_parts, params = subscriber_match_issue_where_parts(
        {
            "event_id": "2",
            "status_filter": "",
            "q": "",
            "facility_q": "",
            "facility_codes": "",
            "exam_month": "",
        }
    )
    sql = " ".join(where_parts)

    assert "MANUAL_CONFIRMED" in sql
    assert "el.subscriber_id IS NOT NULL" in sql
    assert "('identity_hash', 'manual')" in sql
    assert params == ["2"]


def test_resolved_rows_require_current_manual_match_and_filter_query() -> None:
    cursor = Cursor()

    rows = load_subscriber_match_resolved_rows(cursor, event_id=2, query="山田", limit=100)

    assert rows == []
    assert "el.subscriber_match_status = 'MATCHED'" in cursor.sql
    assert "el.subscriber_match_method = 'manual'" in cursor.sql
    assert "latest.new_subscriber_match_status = 'MATCHED'" in cursor.sql
    assert "el.name_full_raw LIKE %s" in cursor.sql
    assert cursor.params[0] == 2
    assert cursor.params[-1] == 100


def test_candidate_insurer_scope_uses_explicit_collation() -> None:
    source = getsource(load_subscriber_match_candidate_rows)

    assert "CONVERT(s.insurer_number USING utf8mb4) COLLATE utf8mb4_unicode_ci" in source
    assert "CONVERT(e.insurer_number USING utf8mb4) COLLATE utf8mb4_unicode_ci" in source
    assert source.index("if not where_parts and not filter_parts:") < source.index("include_other_insurers =")
