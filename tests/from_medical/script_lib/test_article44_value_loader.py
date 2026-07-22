from decimal import Decimal
import re

import pytest

from scripts.from_medical.script_lib.article44_models import (
    CDValue,
    ExpectedValueType,
    PQValue,
    RequiredNamecode,
    STValue,
    ValueInvalidReason,
    ValueState,
)
from scripts.from_medical.script_lib.article44_value_loader import (
    _build_value_map,
    load_article44_value_map,
)


def required(namecode: str, expected_value_type: ExpectedValueType) -> RequiredNamecode:
    return RequiredNamecode(namecode=namecode, expected_value_type=expected_value_type)


def row(
    *,
    namecode: str | None,
    raw_value_type: str | None,
    raw_value: object = None,
    code_value: object = None,
    unit: object = None,
    section_code: object = "01030",
    id: int = 1,
) -> dict[str, object]:
    return {
        "id": id,
        "namecode": namecode,
        "raw_value_type": raw_value_type,
        "raw_value": raw_value,
        "raw_unit": unit,
        "code_value": code_value,
        "section_code": section_code,
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


def build_one_value(
    expected_value_type: ExpectedValueType,
    db_row: dict[str, object],
) -> PQValue | CDValue | STValue:
    value_map = _build_value_map((required("x", expected_value_type),), [db_row])
    return value_map["x"]


@pytest.mark.parametrize("bad_xml_ledger_id", [0, -1])
def test_load_article44_value_map_rejects_invalid_xml_ledger_id(bad_xml_ledger_id: int) -> None:
    cursor = FakeCursor([])

    with pytest.raises(ValueError):
        load_article44_value_map(
            cursor,
            xml_ledger_id=bad_xml_ledger_id,
            required_namecodes=(required("x", ExpectedValueType.PQ),),
        )

    assert cursor.execute_calls == []


def test_build_value_map_rejects_empty_required_namecodes() -> None:
    with pytest.raises(ValueError):
        _build_value_map((), [])


def test_build_value_map_rejects_empty_required_namecode() -> None:
    with pytest.raises(ValueError):
        _build_value_map((required("", ExpectedValueType.PQ),), [])


@pytest.mark.parametrize(
    "required_namecodes",
    [
        (
            required("x", ExpectedValueType.PQ),
            required("x", ExpectedValueType.PQ),
        ),
        (
            required("x", ExpectedValueType.PQ),
            required("x", ExpectedValueType.CD),
        ),
    ],
)
def test_build_value_map_rejects_duplicate_required_namecode(
    required_namecodes: tuple[RequiredNamecode, ...],
) -> None:
    with pytest.raises(ValueError):
        _build_value_map(required_namecodes, [])


def test_build_value_map_rejects_unexpected_row_namecode() -> None:
    with pytest.raises(ValueError):
        _build_value_map(
            (required("x", ExpectedValueType.PQ),),
            [row(namecode="other", raw_value_type="PQ", raw_value="1")],
        )


@pytest.mark.parametrize("bad_namecode", [None, ""])
def test_build_value_map_rejects_missing_or_empty_row_namecode(
    bad_namecode: str | None,
) -> None:
    with pytest.raises(ValueError):
        _build_value_map(
            (required("x", ExpectedValueType.PQ),),
            [row(namecode=bad_namecode, raw_value_type="PQ", raw_value="1")],
        )


def test_build_value_map_rejects_missing_required_row_column() -> None:
    db_row = row(namecode="x", raw_value_type="PQ", raw_value="1")
    del db_row["raw_value_type"]

    with pytest.raises(ValueError):
        _build_value_map((required("x", ExpectedValueType.PQ),), [db_row])


def test_build_value_map_returns_all_required_keys_in_required_order() -> None:
    required_namecodes = (
        required("pq", ExpectedValueType.PQ),
        required("cd", ExpectedValueType.CD),
        required("st", ExpectedValueType.ST),
    )

    value_map = _build_value_map(
        required_namecodes,
        [row(namecode="cd", raw_value_type="CD", code_value="1")],
    )

    assert tuple(value_map) == ("pq", "cd", "st")
    assert value_map["pq"].value_state == ValueState.NOT_FOUND
    assert value_map["cd"].is_valid is True
    assert value_map["st"].value_state == ValueState.NOT_FOUND


def test_build_value_map_is_independent_of_db_row_order() -> None:
    required_namecodes = (
        required("pq", ExpectedValueType.PQ),
        required("cd", ExpectedValueType.CD),
        required("st", ExpectedValueType.ST),
    )
    rows = [
        row(namecode="st", raw_value_type="ST", raw_value="text"),
        row(namecode="pq", raw_value_type="PQ", raw_value="1"),
        row(namecode="cd", raw_value_type="CD", code_value="2"),
    ]

    first = _build_value_map(required_namecodes, rows)
    second = _build_value_map(required_namecodes, tuple(reversed(rows)))

    assert first == second


def test_build_value_map_returns_72_not_found_values() -> None:
    required_namecodes = tuple(
        required(f"namecode_{index:02}", ExpectedValueType.PQ) for index in range(72)
    )

    value_map = _build_value_map(required_namecodes, [])

    assert len(value_map) == 72
    assert all(value.value_state == ValueState.NOT_FOUND for value in value_map.values())


@pytest.mark.parametrize(
    ("expected_value_type", "expected_class", "value_field"),
    [
        (ExpectedValueType.PQ, PQValue, "numeric_value"),
        (ExpectedValueType.CD, CDValue, "code_value"),
        (ExpectedValueType.ST, STValue, "text"),
    ],
)
def test_build_value_map_returns_typed_not_found_value(
    expected_value_type: ExpectedValueType,
    expected_class: type[PQValue | CDValue | STValue],
    value_field: str,
) -> None:
    value = _build_value_map((required("x", expected_value_type),), [])["x"]

    assert isinstance(value, expected_class)
    assert value.value_state == ValueState.NOT_FOUND
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.duplicate_count is None
    assert getattr(value, value_field) is None


@pytest.mark.parametrize(
    ("raw_value", "expected_decimal"),
    [
        ("170", Decimal("170")),
        ("170.5", Decimal("170.5")),
        (" 170.5 ", Decimal("170.5")),
    ],
)
def test_build_value_map_normalizes_valid_pq_values(
    raw_value: str,
    expected_decimal: Decimal,
) -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value=raw_value),
    )

    assert isinstance(value, PQValue)
    assert value.value_state == ValueState.PRESENT
    assert value.raw_value == raw_value
    assert value.numeric_value == expected_decimal
    assert value.is_valid is True


def test_build_value_map_preserves_pq_unit_when_present() -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value="170", unit="cm"),
    )

    assert isinstance(value, PQValue)
    assert value.unit == "cm"


def test_build_value_map_allows_missing_pq_unit() -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value="170"),
    )

    assert isinstance(value, PQValue)
    assert value.unit is None


def test_build_value_map_returns_null_pq_value() -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value=None),
    )

    assert isinstance(value, PQValue)
    assert value.value_state == ValueState.NULL
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.numeric_value is None


@pytest.mark.parametrize("raw_value", ["", "   "])
def test_build_value_map_returns_empty_pq_value(raw_value: str) -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value=raw_value),
    )

    assert isinstance(value, PQValue)
    assert value.value_state == ValueState.EMPTY
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.numeric_value is None


def test_build_value_map_returns_pq_parse_error() -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="PQ", raw_value="測定不能"),
    )

    assert isinstance(value, PQValue)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.PARSE_ERROR
    assert value.numeric_value is None


def test_build_value_map_returns_pq_type_mismatch() -> None:
    value = build_one_value(
        ExpectedValueType.PQ,
        row(namecode="x", raw_value_type="ST", raw_value="170", unit="cm"),
    )

    assert isinstance(value, PQValue)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.TYPE_MISMATCH
    assert value.numeric_value is None
    assert value.unit == "cm"


@pytest.mark.parametrize("raw_value_type", ["CD", "CO"])
def test_build_value_map_normalizes_valid_cd_and_co_values(raw_value_type: str) -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type=raw_value_type, raw_value="raw", code_value="1"),
    )

    assert isinstance(value, CDValue)
    assert value.value_state == ValueState.PRESENT
    assert value.code_value == "1"
    assert value.is_valid is True


def test_build_value_map_normalizes_multi_digit_cd_value() -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type="CD", code_value="10"),
    )

    assert isinstance(value, CDValue)
    assert value.code_value == "10"
    assert value.is_valid is True


@pytest.mark.parametrize("code_value", ["0", "01", "A1"])
def test_build_value_map_returns_cd_format_error(code_value: str) -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type="CD", raw_value="raw", code_value=code_value),
    )

    assert isinstance(value, CDValue)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.FORMAT_ERROR
    assert value.code_value == code_value


def test_build_value_map_returns_null_cd_value() -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type="CD", raw_value="raw", code_value=None),
    )

    assert isinstance(value, CDValue)
    assert value.value_state == ValueState.NULL
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.code_value is None


@pytest.mark.parametrize("code_value", ["", "   "])
def test_build_value_map_returns_empty_cd_value(code_value: str) -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type="CD", raw_value="raw", code_value=code_value),
    )

    assert isinstance(value, CDValue)
    assert value.value_state == ValueState.EMPTY
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.code_value is None


def test_build_value_map_returns_cd_type_mismatch() -> None:
    value = build_one_value(
        ExpectedValueType.CD,
        row(namecode="x", raw_value_type="PQ", raw_value="raw", code_value="1"),
    )

    assert isinstance(value, CDValue)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.TYPE_MISMATCH


def test_build_value_map_normalizes_valid_st_value() -> None:
    value = build_one_value(
        ExpectedValueType.ST,
        row(namecode="x", raw_value_type="ST", raw_value="finding text"),
    )

    assert isinstance(value, STValue)
    assert value.value_state == ValueState.PRESENT
    assert value.text == "finding text"
    assert value.is_valid is True


@pytest.mark.parametrize(
    ("raw_text", "expected_text"),
    [
        ("ＡＢＣ１２３", "ABC123"),
        ("A\r\nB", "A B"),
        ("A\rB", "A B"),
        ("A\nB", "A B"),
        ("A\tB", "A B"),
        ("A   B", "A B"),
        ("A　B", "A B"),
    ],
)
def test_build_value_map_normalizes_st_text(raw_text: str, expected_text: str) -> None:
    value = build_one_value(
        ExpectedValueType.ST,
        row(namecode="x", raw_value_type="ST", raw_value=raw_text),
    )

    assert isinstance(value, STValue)
    assert value.text == expected_text
    assert value.is_valid is True


def test_build_value_map_returns_null_st_value() -> None:
    value = build_one_value(
        ExpectedValueType.ST,
        row(namecode="x", raw_value_type="ST", raw_value=None),
    )

    assert isinstance(value, STValue)
    assert value.value_state == ValueState.NULL
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.text is None


def test_build_value_map_returns_empty_st_value() -> None:
    value = build_one_value(
        ExpectedValueType.ST,
        row(namecode="x", raw_value_type="ST", raw_value=" \r\n\t　"),
    )

    assert isinstance(value, STValue)
    assert value.value_state == ValueState.EMPTY
    assert value.is_valid is False
    assert value.invalid_reason is None
    assert value.text == ""


def test_build_value_map_returns_st_type_mismatch() -> None:
    value = build_one_value(
        ExpectedValueType.ST,
        row(namecode="x", raw_value_type="PQ", raw_value="Ａ"),
    )

    assert isinstance(value, STValue)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.TYPE_MISMATCH
    assert value.text == "A"


@pytest.mark.parametrize(
    ("expected_value_type", "rows", "expected_class", "duplicate_count"),
    [
        (
            ExpectedValueType.PQ,
            [
                row(namecode="x", raw_value_type="PQ", raw_value="1", id=1),
                row(namecode="x", raw_value_type="PQ", raw_value="2", id=2),
            ],
            PQValue,
            2,
        ),
        (
            ExpectedValueType.CD,
            [
                row(namecode="x", raw_value_type="CD", code_value="1", id=1),
                row(namecode="x", raw_value_type="CD", code_value="2", id=2),
            ],
            CDValue,
            2,
        ),
        (
            ExpectedValueType.ST,
            [
                row(namecode="x", raw_value_type="ST", raw_value="a", id=1),
                row(namecode="x", raw_value_type="ST", raw_value="b", id=2),
                row(namecode="x", raw_value_type="ST", raw_value="c", id=3),
            ],
            STValue,
            3,
        ),
    ],
)
def test_build_value_map_returns_duplicate_value(
    expected_value_type: ExpectedValueType,
    rows: list[dict[str, object]],
    expected_class: type[PQValue | CDValue | STValue],
    duplicate_count: int,
) -> None:
    value = _build_value_map((required("x", expected_value_type),), rows)["x"]

    assert isinstance(value, expected_class)
    assert value.value_state == ValueState.PRESENT
    assert value.is_valid is False
    assert value.invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    assert value.duplicate_count == duplicate_count


def test_build_value_map_returns_pq_duplicate_shape() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.PQ),),
        [
            row(namecode="x", raw_value_type="PQ", raw_value="1", unit="cm", id=1),
            row(namecode="x", raw_value_type="PQ", raw_value="2", unit="cm", id=2),
        ],
    )["x"]

    assert isinstance(value, PQValue)
    assert value.raw_value is None
    assert value.numeric_value is None
    assert value.unit is None


def test_build_value_map_treats_multiple_rows_as_duplicate_even_when_one_is_null() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.PQ),),
        [
            row(namecode="x", raw_value_type="PQ", raw_value=None, id=1),
            row(namecode="x", raw_value_type="PQ", raw_value="2", id=2),
        ],
    )["x"]

    assert value.invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    assert value.duplicate_count == 2


def test_build_value_map_continues_after_duplicate_namecode() -> None:
    value_map = _build_value_map(
        (
            required("duplicate", ExpectedValueType.CD),
            required("normal", ExpectedValueType.PQ),
        ),
        [
            row(namecode="duplicate", raw_value_type="CD", code_value="1", id=1),
            row(namecode="duplicate", raw_value_type="CD", code_value="2", id=2),
            row(namecode="normal", raw_value_type="PQ", raw_value="3", id=3),
        ],
    )

    assert value_map["duplicate"].invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    assert value_map["normal"].is_valid is True


def test_build_value_map_prefers_article44_section_when_other_section_has_same_namecode() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.ST),),
        [
            row(
                namecode="x",
                raw_value_type="ST",
                raw_value="特定健診側",
                section_code="01010",
                id=1,
            ),
            row(
                namecode="x",
                raw_value_type="ST",
                raw_value="労安法側",
                section_code="01030",
                id=2,
            ),
            row(
                namecode="x",
                raw_value_type="ST",
                raw_value="がん検診側",
                section_code="01060",
                id=3,
            ),
        ],
    )["x"]

    assert isinstance(value, STValue)
    assert value.is_valid is True
    assert value.raw_text == "労安法側"
    assert value.invalid_reason is None


def test_build_value_map_returns_duplicate_when_article44_section_has_multiple_rows() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.CD),),
        [
            row(namecode="x", raw_value_type="CD", code_value="1", section_code="01010", id=1),
            row(namecode="x", raw_value_type="CD", code_value="2", section_code="01030", id=2),
            row(namecode="x", raw_value_type="CD", code_value="3", section_code="01030", id=3),
        ],
    )["x"]

    assert isinstance(value, CDValue)
    assert value.invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    assert value.duplicate_count == 2


def test_build_value_map_falls_back_to_single_non_article44_section_row() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.PQ),),
        [
            row(
                namecode="x",
                raw_value_type="PQ",
                raw_value="170",
                section_code="01010",
                id=1,
            ),
        ],
    )["x"]

    assert isinstance(value, PQValue)
    assert value.is_valid is True
    assert value.numeric_value == Decimal("170")


def test_build_value_map_returns_duplicate_when_no_article44_section_and_multiple_rows() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.ST),),
        [
            row(namecode="x", raw_value_type="ST", raw_value="a", section_code="01010", id=1),
            row(namecode="x", raw_value_type="ST", raw_value="b", section_code="01060", id=2),
        ],
    )["x"]

    assert isinstance(value, STValue)
    assert value.invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE
    assert value.duplicate_count == 2


def test_build_value_map_prefers_article44_section_over_null_section_row() -> None:
    value = _build_value_map(
        (required("x", ExpectedValueType.CD),),
        [
            row(namecode="x", raw_value_type="CD", code_value="1", section_code=None, id=1),
            row(namecode="x", raw_value_type="CD", code_value="2", section_code="01030", id=2),
        ],
    )["x"]

    assert isinstance(value, CDValue)
    assert value.is_valid is True
    assert value.code_value == "2"


def test_load_article44_value_map_uses_single_sql_and_fake_cursor_contract() -> None:
    cursor = FakeCursor(
        [
            row(namecode="x", raw_value_type="PQ", raw_value="1"),
            row(namecode="y", raw_value_type="CD", code_value="2"),
        ]
    )
    required_namecodes = (
        required("x", ExpectedValueType.PQ),
        required("y", ExpectedValueType.CD),
    )

    value_map = load_article44_value_map(
        cursor,
        xml_ledger_id=123,
        required_namecodes=required_namecodes,
    )

    assert len(cursor.execute_calls) == 1
    assert cursor.fetchall_calls == 1
    sql, params = cursor.execute_calls[0]
    compact_sql = re.sub(r"\s+", " ", sql).strip()

    assert "exam_item_values" in compact_sql
    assert "ledger_type = 'XML'" in compact_sql
    assert "ledger_id = %s" in compact_sql
    assert "namecode IN" in compact_sql
    assert "section_code" in compact_sql
    assert compact_sql.count("%s") == 1 + len(required_namecodes)
    assert params == (123, "x", "y")
    assert " JOIN " not in compact_sql.upper()
    assert " DISTINCT " not in compact_sql.upper()
    assert " GROUP BY " not in compact_sql.upper()
    assert "MAX(" not in compact_sql.upper()

    assert value_map["x"].is_valid is True
    assert value_map["y"].is_valid is True
    assert cursor.commit_calls == 0
    assert cursor.rollback_calls == 0
    assert cursor.close_calls == 0
