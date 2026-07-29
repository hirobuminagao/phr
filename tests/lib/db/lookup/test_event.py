from scripts.lib.db.lookup.event import get_event_insurer_number


class FakeCursor:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self) -> dict[str, object] | None:
        return self.row


def test_get_event_insurer_number_returns_trimmed_value() -> None:
    cursor = FakeCursor({"insurer_number": " 06139463 "})

    result = get_event_insurer_number(cursor, event_id=2, dev_db="dev_phr")

    assert result == "06139463"
    assert cursor.execute_calls[0][1] == (2,)


def test_get_event_insurer_number_returns_none_when_event_is_missing() -> None:
    cursor = FakeCursor(None)

    assert get_event_insurer_number(cursor, event_id=2) is None
