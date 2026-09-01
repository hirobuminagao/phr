from __future__ import annotations

from typing import Any
from decimal import Decimal

from starlette.responses import JSONResponse

from apps.health_exam_admin.main import (
    count_facility_master_admin_rows,
    facility_master_search_item,
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


def test_facility_search_item_converts_decimal_alias_counts_for_json() -> None:
    item = facility_master_search_item(
        {
            "exam_facility_id": 1,
            "exam_facility_name": "マイヘルス",
            "alias_count": Decimal("2"),
            "active_alias_count": Decimal("1"),
        }
    )

    response = JSONResponse({"items": [item]})

    assert response.status_code == 200
    assert item["alias_count"] == 2
    assert item["active_alias_count"] == 1
