from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.support_scripts.capture_snapshot import default_query_path, json_default, load_select_sql


def test_load_select_sql_accepts_single_select(tmp_path: Path) -> None:
    query = tmp_path / "001.sql"
    query.write_text("SELECT 1 AS target_id;\n", encoding="utf-8")

    assert load_select_sql(query) == "SELECT 1 AS target_id"


def test_load_select_sql_rejects_multiple_statements(tmp_path: Path) -> None:
    query = tmp_path / "001.sql"
    query.write_text("SELECT 1; DELETE FROM support_incidents;", encoding="utf-8")

    with pytest.raises(ValueError, match="exactly one SELECT"):
        load_select_sql(query)


def test_json_default_serializes_database_value_types() -> None:
    assert json_default(date(2026, 9, 1)) == "2026-09-01"
    assert json_default(Decimal("1.20")) == "1.20"


def test_default_query_path_uses_after_query_for_comparison() -> None:
    assert default_query_path(1, "BEFORE").name == "001.sql"
    assert default_query_path(1, "AFTER").name == "001_after.sql"
