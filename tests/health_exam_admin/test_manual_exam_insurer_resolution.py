from apps.health_exam_admin.main import resolve_manual_exam_entry_insurer_number


class Cursor:
    def __init__(self, rows: list[dict[str, object] | None]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> dict[str, object] | None:
        return self.rows.pop(0) if self.rows else None


def test_all_zero_manual_insurer_uses_linked_case_correction() -> None:
    cursor = Cursor([{"insurer_number_export_value": "06139463", "insurer_number": "00000000"}])

    value, source = resolve_manual_exam_entry_insurer_number(
        cursor,
        event_id=2,
        source_value="00000000",
        exam_export_case_id=51072,
        subscriber_id=10,
    )

    assert value == "06139463"
    assert source == "LINKED_CASE"


def test_valid_manual_insurer_does_not_query_fallbacks() -> None:
    cursor = Cursor([])

    value, source = resolve_manual_exam_entry_insurer_number(
        cursor,
        event_id=2,
        source_value="06139463",
        exam_export_case_id=51072,
        subscriber_id=10,
    )

    assert value == "06139463"
    assert source == "SOURCE"
    assert cursor.calls == []
