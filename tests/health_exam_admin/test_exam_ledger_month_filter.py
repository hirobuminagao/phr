from pathlib import Path

from apps.health_exam_admin.main import (
    load_exam_ledger_month_options,
    load_exam_ledger_rows,
)


class ExamLedgerCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return []


def test_exam_ledger_rows_filter_multiple_exam_months() -> None:
    cur = ExamLedgerCursor()

    load_exam_ledger_rows(
        cur,
        filters={"event_id": "2", "exam_month": "2026-05, 2026-06"},
        limit=200,
    )

    sql, params = cur.calls[0]
    assert "DATE_FORMAT(exam_date, '%Y-%m') IN (%s, %s)" in sql
    assert params == ("2", "2026-05", "2026-06", 200)


def test_exam_ledger_month_options_are_limited_to_event() -> None:
    cur = ExamLedgerCursor()

    load_exam_ledger_month_options(cur, event_id="2")

    sql, params = cur.calls[0]
    assert "event_id = %s" in sql
    assert "COUNT(*) AS ledger_count" in sql
    assert params == ("2", 36)


def test_exam_ledger_page_has_multi_month_picker() -> None:
    template = Path("apps/health_exam_admin/templates/exam_ledgers.html").read_text(encoding="utf-8")

    assert "受診月" in template
    assert 'name="exam_month"' in template
    assert "data-month-picker" in template
    assert "data-month-picker-option" in template
    assert "{{ option.ledger_count }}件" in template
