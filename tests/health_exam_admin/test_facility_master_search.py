from __future__ import annotations

from typing import Any

from apps.health_exam_admin.main import (
    count_facility_master_admin_rows,
    load_facility_master_admin_rows,
)


class FakeCursor:
    def __init__(self, *, one: dict[str, Any] | None = None) -> None:
        self.one = one
        self.sql = ""
        self.params: list[Any] = []

    def execute(self, sql: str, params: list[Any]) -> None:
        self.sql = sql
        self.params = params

    def fetchone(self) -> dict[str, Any] | None:
        return self.one

    def fetchall(self) -> list[dict[str, Any]]:
        return []


def test_facility_count_accepts_search_filters_without_alias_sort_option() -> None:
    cur = FakeCursor(one={"cnt": 3})

    count = count_facility_master_admin_rows(cur, keyword="札幌", prefecture="北海道")

    assert count == 3
    assert "COUNT(*)" in cur.sql
    assert cur.params == ["北海道%", "%札幌%", "%札幌%", "%札幌%", "%札幌%", "%札幌%", "%札幌%"]


def test_facility_rows_can_prioritize_alias_registered_facilities() -> None:
    cur = FakeCursor()

    load_facility_master_admin_rows(cur, keyword="札幌", prefer_alias_registered=True)

    assert "COALESCE(alias_counts.alias_count, 0) > 0 DESC" in cur.sql
    assert cur.params[-1] == 500
