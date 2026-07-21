from pathlib import Path
import re

import pytest

from scripts.from_medical.script_lib.article44_models import (
    ExpectedValueType,
    RequiredNamecode,
)
from scripts.from_medical.script_lib.article44_required_namecodes import (
    ARTICLE44_GROUP_CODE,
    _build_required_namecodes,
    fetch_article44_required_namecodes,
)


def row(
    *,
    namecode: object = "9N001000000000001",
    value_type: object = "PQ",
    method: object = "9N00100000",
    identity_code: object = "9N001",
    priority: object = 10,
) -> dict[str, object]:
    return {
        "namecode": namecode,
        "value_type": value_type,
        "method": method,
        "identity_code": identity_code,
        "priority": priority,
    }


class FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchall_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.execute_calls.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        self.fetchall_calls += 1
        return self.rows

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


@pytest.mark.parametrize(
    ("value_type", "expected_value_type"),
    [
        ("PQ", ExpectedValueType.PQ),
        ("CD", ExpectedValueType.CD),
        ("ST", ExpectedValueType.ST),
        ("CO", ExpectedValueType.CD),
        (" PQ ", ExpectedValueType.PQ),
    ],
)
def test_build_required_namecodes_converts_value_type(
    value_type: str,
    expected_value_type: ExpectedValueType,
) -> None:
    results = _build_required_namecodes([row(value_type=value_type)])

    assert results == (
        RequiredNamecode(
            namecode="9N001000000000001",
            expected_value_type=expected_value_type,
        ),
    )


def test_build_required_namecodes_preserves_input_order() -> None:
    rows = [
        row(namecode="first", value_type="PQ", priority=20),
        row(namecode="second", value_type="CD", priority=10),
        row(namecode="third", value_type="ST", priority=30),
    ]

    results = _build_required_namecodes(rows)

    assert tuple(result.namecode for result in results) == ("first", "second", "third")


def test_build_required_namecodes_converts_72_rows() -> None:
    rows = [
        row(
            namecode=f"namecode_{index:02}",
            value_type="PQ",
            method=f"method_{index:02}",
            identity_code=f"id_{index:02}",
            priority=index,
        )
        for index in range(72)
    ]

    results = _build_required_namecodes(rows)

    assert len(results) == 72
    assert all(isinstance(result, RequiredNamecode) for result in results)


def test_build_required_namecodes_rejects_empty_rows() -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes([])


@pytest.mark.parametrize("column", ["namecode", "value_type", "method", "identity_code"])
@pytest.mark.parametrize("bad_value", [None, "", "   "])
def test_build_required_namecodes_rejects_required_text_column(
    column: str,
    bad_value: object,
) -> None:
    db_row = row()
    db_row[column] = bad_value

    with pytest.raises(ValueError):
        _build_required_namecodes([db_row])


def test_build_required_namecodes_rejects_missing_priority() -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes([row(priority=None)])


def test_build_required_namecodes_allows_non_integer_priority_as_present_value() -> None:
    results = _build_required_namecodes([row(priority="not-integer")])

    assert results[0].namecode == "9N001000000000001"


@pytest.mark.parametrize("value_type", ["pq", "cd", "st", "co", "UNKNOWN", "P Q"])
def test_build_required_namecodes_rejects_invalid_value_type(value_type: str) -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes([row(value_type=value_type)])


@pytest.mark.parametrize("missing_column", ["namecode", "value_type", "method", "identity_code", "priority"])
def test_build_required_namecodes_rejects_missing_columns(missing_column: str) -> None:
    db_row = row()
    del db_row[missing_column]

    with pytest.raises(ValueError):
        _build_required_namecodes([db_row])


def test_build_required_namecodes_deduplicates_identical_namecode_definition() -> None:
    results = _build_required_namecodes(
        [
            row(namecode="same", value_type="PQ", method="m1", identity_code="i1", priority=10),
            row(namecode="same", value_type="PQ", method="m1", identity_code="i1", priority=20),
        ]
    )

    assert results == (RequiredNamecode("same", ExpectedValueType.PQ),)


def test_build_required_namecodes_keeps_first_position_when_deduplicating() -> None:
    results = _build_required_namecodes(
        [
            row(namecode="first", value_type="PQ", method="m1", identity_code="i1", priority=10),
            row(namecode="second", value_type="ST", method="m2", identity_code="i2", priority=20),
            row(namecode="first", value_type="PQ", method="m1", identity_code="i1", priority=30),
        ]
    )

    assert tuple(result.namecode for result in results) == ("first", "second")


def test_build_required_namecodes_rejects_duplicate_with_different_expected_value_type() -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes(
            [
                row(namecode="same", value_type="PQ", method="m1", identity_code="i1"),
                row(namecode="same", value_type="CD", method="m1", identity_code="i1"),
            ]
        )


def test_build_required_namecodes_rejects_duplicate_with_different_method() -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes(
            [
                row(namecode="same", value_type="PQ", method="m1", identity_code="i1"),
                row(namecode="same", value_type="PQ", method="m2", identity_code="i1"),
            ]
        )


def test_build_required_namecodes_rejects_duplicate_with_different_identity_code() -> None:
    with pytest.raises(ValueError):
        _build_required_namecodes(
            [
                row(namecode="same", value_type="PQ", method="m1", identity_code="i1"),
                row(namecode="same", value_type="PQ", method="m1", identity_code="i2"),
            ]
        )


def test_build_required_namecodes_deduplicates_cd_and_co_when_metadata_matches() -> None:
    results = _build_required_namecodes(
        [
            row(namecode="same", value_type="CD", method="m1", identity_code="i1"),
            row(namecode="same", value_type="CO", method="m1", identity_code="i1"),
        ]
    )

    assert results == (RequiredNamecode("same", ExpectedValueType.CD),)


def test_build_required_namecodes_returns_required_namecode_contract_only() -> None:
    results = _build_required_namecodes([row()])
    result = results[0]

    assert isinstance(results, tuple)
    assert isinstance(result, RequiredNamecode)
    assert result.namecode == "9N001000000000001"
    assert result.expected_value_type == ExpectedValueType.PQ
    assert not hasattr(result, "method")
    assert not hasattr(result, "identity_code")
    assert not hasattr(result, "priority")


@pytest.mark.parametrize("dev_db", ["dev_phr", "dev`phr"])
def test_fetch_article44_required_namecodes_uses_expected_sql_contract(dev_db: str) -> None:
    cursor = FakeCursor([row()])

    results = fetch_article44_required_namecodes(cursor, dev_db=dev_db)

    assert len(cursor.execute_calls) == 1
    assert cursor.fetchall_calls == 1
    sql, params = cursor.execute_calls[0]
    compact_sql = re.sub(r"\s+", " ", sql).strip()
    upper_sql = compact_sql.upper()

    assert "exam_item_group_members" in compact_sql
    assert "exam_item_master" not in compact_sql
    assert " JOIN " not in upper_sql
    assert "group_code = %s" in compact_sql
    assert params == (ARTICLE44_GROUP_CODE,)
    assert "namecode" in compact_sql
    assert "value_type" in compact_sql
    assert "method" in compact_sql
    assert "identity_code" in compact_sql
    assert "priority" in compact_sql
    assert "ORDER BY priority, namecode" in compact_sql
    assert ARTICLE44_GROUP_CODE not in compact_sql
    assert f"`{dev_db.replace('`', '``')}`.exam_item_group_members" in compact_sql
    assert results == (RequiredNamecode("9N001000000000001", ExpectedValueType.PQ),)
    assert cursor.commit_calls == 0
    assert cursor.rollback_calls == 0
    assert cursor.close_calls == 0


def test_article44_group_code_matches_seed_constant() -> None:
    assert ARTICLE44_GROUP_CODE == "v2_2026_ARTICLE44_CHECK_ITEMS"


def test_article44_seed_file_exists() -> None:
    seed_path = (
        Path(__file__).parents[3]
        / "sql"
        / "seed"
        / "dev_phr"
        / "0015_dev_phr__article44_check_items_v2_2026.sql"
    )

    assert seed_path.exists()
