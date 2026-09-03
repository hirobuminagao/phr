from apps.health_exam_admin.main import load_subscriber_match_resolution_counts


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
