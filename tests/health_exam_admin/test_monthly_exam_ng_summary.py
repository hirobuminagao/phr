from typing import Any

import apps.health_exam_admin.main as main


class Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[dict[str, Any]]:
        return []


def test_monthly_ng_report_applies_all_selected_months(monkeypatch: Any) -> None:
    monkeypatch.setattr(main, "load_facility_summary_rows", lambda *_args, **_kwargs: [])
    cur = Cursor()

    main.load_monthly_exam_ng_report(cur, event_id="2", exam_month="2026-05,2026-07")

    assert len(cur.calls) == 2
    for sql, params in cur.calls:
        assert "DATE_FORMAT(eec.exam_date, '%Y-%m') IN (%s, %s)" in sql
        assert params == ("2", "2026-05", "2026-07")
