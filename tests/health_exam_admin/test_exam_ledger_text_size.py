from apps.health_exam_admin.main import load_exam_item_value_rows


class Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
        pass

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows


def test_exam_ledger_st_rows_include_mhlw_text_size() -> None:
    rows = load_exam_item_value_rows(
        Cursor(
            [
                {"raw_value_type": "ST", "raw_value": "ABCあいう"},
                {"raw_value_type": "PQ", "raw_value": "123"},
            ]
        ),
        exam_ledger_id=1,
    )

    assert rows[0]["text_character_count"] == 6
    assert rows[0]["text_byte_count"] == 9
    assert rows[0]["text_byte_limit"] == 256
    assert rows[0]["text_byte_limit_exceeded"] is False
    assert "text_byte_count" not in rows[1]


def test_exam_ledger_st_rows_mark_values_over_limit() -> None:
    rows = load_exam_item_value_rows(
        Cursor([{"raw_value_type": "ST", "raw_value": "あ" * 129}]),
        exam_ledger_id=1,
    )

    assert rows[0]["text_byte_count"] == 258
    assert rows[0]["text_byte_limit_exceeded"] is True
