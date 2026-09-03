from apps.health_exam_admin.main import load_subscriber_match_resolution_counts, subscriber_match_issue_where_parts


class Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple[object, ...] = ()

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchone(self) -> dict[str, object]:
        return {"resolved_person_count": 12, "resolved_ledger_count": 15}


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
