"""Load and normalize Article 44 examination values for one ledger."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
import re
from typing import Any
import unicodedata

from scripts.from_medical.script_lib.article44_models import (
    CDValue,
    ExpectedValueType,
    PQValue,
    RequiredNamecode,
    STValue,
    ValueInvalidReason,
    ValueMap,
    ValueState,
)
from scripts.lib.examination.lookup import qname


ARTICLE44_SECTION_CODE = "01030"


def load_article44_value_map(
    cursor: Any,
    *,
    xml_ledger_id: int | None = None,
    ledger_type: str = "XML",
    ledger_id: int | None = None,
    required_namecodes: tuple[RequiredNamecode, ...],
    result_db: str = "health_exam_result",
    dev_db: str = "dev_phr",
) -> ValueMap:
    """Load required Article 44 values for one imported exam ledger or export case."""

    resolved_ledger_id = ledger_id if ledger_id is not None else xml_ledger_id
    if (
        not isinstance(resolved_ledger_id, int)
        or isinstance(resolved_ledger_id, bool)
        or resolved_ledger_id <= 0
    ):
        raise ValueError(f"ledger_id must be a positive integer: {resolved_ledger_id!r}")
    resolved_ledger_type = ledger_type.strip().upper() if isinstance(ledger_type, str) else ""
    if resolved_ledger_type not in {"EXAM", "XML", "CSV", "EXPORT_CASE"}:
        raise ValueError(f"ledger_type must be EXAM, XML, CSV, or EXPORT_CASE: {ledger_type!r}")
    _validate_required_namecodes(required_namecodes)

    namecodes = tuple(required.namecode for required in required_namecodes)
    placeholders = ", ".join(["%s"] * len(namecodes))
    if resolved_ledger_type == "EXPORT_CASE":
        cursor.execute(
            f"""
            SELECT
              cv.`exam_export_case_id` AS ledger_id,
              cv.`exam_export_case_value_id` AS id,
              cv.`namecode`,
              im.`xml_value_type` AS raw_value_type,
              cv.`normalized_value` AS raw_value,
              cv.`normalized_unit` AS raw_unit,
              cv.`normalized_value`,
              cv.`normalized_unit`,
              cv.`code_value`,
              COALESCE(NULLIF(cv.`section_code`, ''), im.`cda_section_code_default`) AS section_code
            FROM {qname(result_db)}.exam_export_case_values AS cv
            LEFT JOIN {qname(dev_db)}.exam_item_master AS im
              ON im.`namecode` = cv.`namecode`
            WHERE cv.`exam_export_case_id` = %s
              AND cv.`namecode` IN ({placeholders})
            ORDER BY cv.`namecode`, cv.`exam_export_case_value_id`
            """,
            (resolved_ledger_id, *namecodes),
        )
    else:
        cursor.execute(
            f"""
            SELECT
              ledger_id,
              id,
              namecode,
              raw_value_type,
              raw_value,
              raw_unit,
              normalized_value,
              normalized_unit,
              code_value,
              section_code
            FROM {qname(result_db)}.exam_item_values
            WHERE ledger_type = %s
              AND ledger_id = %s
              AND namecode IN ({placeholders})
            ORDER BY namecode, id
            """,
            (resolved_ledger_type, resolved_ledger_id, *namecodes),
        )
    return _build_value_map(required_namecodes, cursor.fetchall())


def _build_value_map(
    required_namecodes: tuple[RequiredNamecode, ...],
    rows: Iterable[Mapping[str, object]],
) -> ValueMap:
    _validate_required_namecodes(required_namecodes)
    required_by_namecode = {
        required.namecode: required.expected_value_type for required in required_namecodes
    }
    rows_by_namecode = _group_rows_by_namecode(rows, required_by_namecode.keys())

    value_map: ValueMap = {}
    for required in required_namecodes:
        namecode_rows = rows_by_namecode.get(required.namecode, ())
        if not namecode_rows:
            value_map[required.namecode] = _not_found_value(required.expected_value_type)
            continue

        selected_rows = _select_article44_section_rows(namecode_rows)
        if len(selected_rows) >= 2:
            value_map[required.namecode] = _duplicate_value(
                required.expected_value_type,
                len(selected_rows),
            )
        else:
            value_map[required.namecode] = _build_value(
                required.expected_value_type,
                selected_rows[0],
            )
    return value_map


def _select_article44_section_rows(
    rows: tuple[Mapping[str, object], ...],
) -> tuple[Mapping[str, object], ...]:
    article44_rows = tuple(
        row for row in rows if _section_code(row) == ARTICLE44_SECTION_CODE
    )
    if article44_rows:
        return article44_rows
    return rows


def _validate_required_namecodes(required_namecodes: tuple[RequiredNamecode, ...]) -> None:
    if not required_namecodes:
        raise ValueError("required_namecodes must not be empty")

    seen: dict[str, ExpectedValueType] = {}
    for required in required_namecodes:
        namecode = required.namecode.strip() if isinstance(required.namecode, str) else ""
        if namecode == "":
            raise ValueError(f"required namecode must not be empty: {required!r}")
        if required.expected_value_type not in {
            ExpectedValueType.PQ,
            ExpectedValueType.CD,
            ExpectedValueType.ST,
        }:
            raise ValueError(f"unknown expected value type: {required.expected_value_type!r}")
        previous = seen.get(namecode)
        if previous is not None:
            raise ValueError(
                f"duplicate required namecode: namecode={namecode} "
                f"previous={previous.value} current={required.expected_value_type.value}"
            )
        seen[namecode] = required.expected_value_type


def _group_rows_by_namecode(
    rows: Iterable[Mapping[str, object]],
    required_namecodes: Iterable[str],
) -> dict[str, tuple[Mapping[str, object], ...]]:
    required_set = set(required_namecodes)
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        namecode = _required_row_text(row, "namecode")
        if namecode not in required_set:
            raise ValueError(f"SQL returned unexpected namecode: {namecode}")
        _require_columns(
            row,
            (
                "raw_value_type",
                "raw_value",
                "raw_unit",
                "normalized_value",
                "normalized_unit",
                "code_value",
                "section_code",
            ),
        )
        grouped[namecode].append(row)
    return {namecode: tuple(namecode_rows) for namecode, namecode_rows in grouped.items()}


def _build_value(
    expected_value_type: ExpectedValueType,
    row: Mapping[str, object],
) -> PQValue | CDValue | STValue:
    db_value_type = _db_value_type(row)
    if expected_value_type == ExpectedValueType.PQ:
        if db_value_type != "PQ":
            return _type_mismatch_value(expected_value_type, row)
        return _build_pq_value(row)
    if expected_value_type == ExpectedValueType.CD:
        if db_value_type not in {"CD", "CO"}:
            return _type_mismatch_value(expected_value_type, row)
        return _build_cd_value(row)
    if expected_value_type == ExpectedValueType.ST:
        if db_value_type != "ST":
            return _type_mismatch_value(expected_value_type, row)
        return _build_st_value(row)
    raise ValueError(f"unknown expected value type: {expected_value_type!r}")


def _not_found_value(expected_value_type: ExpectedValueType) -> PQValue | CDValue | STValue:
    if expected_value_type == ExpectedValueType.PQ:
        return PQValue(ValueState.NOT_FOUND, None, None, None, False, None, None)
    if expected_value_type == ExpectedValueType.CD:
        return CDValue(ValueState.NOT_FOUND, None, None, False, None, None)
    if expected_value_type == ExpectedValueType.ST:
        return STValue(ValueState.NOT_FOUND, None, None, False, None, None)
    raise ValueError(f"unknown expected value type: {expected_value_type!r}")


def _duplicate_value(
    expected_value_type: ExpectedValueType,
    count: int,
) -> PQValue | CDValue | STValue:
    if expected_value_type == ExpectedValueType.PQ:
        return PQValue(
            ValueState.PRESENT,
            None,
            None,
            None,
            False,
            ValueInvalidReason.DUPLICATE_NAMECODE,
            count,
        )
    if expected_value_type == ExpectedValueType.CD:
        return CDValue(
            ValueState.PRESENT,
            None,
            None,
            False,
            ValueInvalidReason.DUPLICATE_NAMECODE,
            count,
        )
    if expected_value_type == ExpectedValueType.ST:
        return STValue(
            ValueState.PRESENT,
            None,
            None,
            False,
            ValueInvalidReason.DUPLICATE_NAMECODE,
            count,
        )
    raise ValueError(f"unknown expected value type: {expected_value_type!r}")


def _type_mismatch_value(
    expected_value_type: ExpectedValueType,
    row: Mapping[str, object],
) -> PQValue | CDValue | STValue:
    raw_value = _optional_text(row, "raw_value")
    if expected_value_type == ExpectedValueType.PQ:
        return PQValue(
            ValueState.PRESENT,
            raw_value,
            None,
            _optional_text(row, "raw_unit"),
            False,
            ValueInvalidReason.TYPE_MISMATCH,
            None,
        )
    if expected_value_type == ExpectedValueType.CD:
        return CDValue(
            ValueState.PRESENT,
            raw_value,
            _optional_text(row, "code_value"),
            False,
            ValueInvalidReason.TYPE_MISMATCH,
            None,
        )
    if expected_value_type == ExpectedValueType.ST:
        return STValue(
            ValueState.PRESENT,
            raw_value,
            _normalize_st_text(raw_value) if raw_value is not None else None,
            False,
            ValueInvalidReason.TYPE_MISMATCH,
            None,
        )
    raise ValueError(f"unknown expected value type: {expected_value_type!r}")


def _build_pq_value(row: Mapping[str, object]) -> PQValue:
    raw_value = _optional_text(row, "raw_value")
    value = _preferred_normalized_text(row)
    unit = _optional_text(row, "normalized_unit") or _optional_text(row, "raw_unit")
    if value is None:
        return PQValue(ValueState.NULL, None, None, unit, False, None, None)

    stripped = value.strip()
    if stripped == "":
        return PQValue(ValueState.EMPTY, raw_value, None, unit, False, None, None)

    try:
        numeric_value = Decimal(stripped)
    except InvalidOperation:
        return PQValue(
            ValueState.PRESENT,
            raw_value,
            None,
            unit,
            False,
            ValueInvalidReason.PARSE_ERROR,
            None,
        )

    return PQValue(ValueState.PRESENT, raw_value, numeric_value, unit, True, None, None)


def _build_cd_value(row: Mapping[str, object]) -> CDValue:
    raw_value = _optional_text(row, "raw_value")
    code_value = _optional_text(row, "code_value")
    if code_value is None:
        return CDValue(ValueState.NULL, raw_value, None, False, None, None)

    stripped = code_value.strip()
    if stripped == "":
        return CDValue(ValueState.EMPTY, raw_value, None, False, None, None)
    if not re.fullmatch(r"[1-9][0-9]*", stripped):
        return CDValue(
            ValueState.PRESENT,
            raw_value,
            stripped,
            False,
            ValueInvalidReason.FORMAT_ERROR,
            None,
        )

    return CDValue(ValueState.PRESENT, raw_value, stripped, True, None, None)


def _build_st_value(row: Mapping[str, object]) -> STValue:
    raw_text = _optional_text(row, "raw_value")
    value = _preferred_normalized_text(row)
    if value is None:
        return STValue(ValueState.NULL, None, None, False, None, None)

    text = _normalize_st_text(value)
    if text == "":
        return STValue(ValueState.EMPTY, raw_text, "", False, None, None)

    return STValue(ValueState.PRESENT, raw_text, text, True, None, None)


def _preferred_normalized_text(row: Mapping[str, object]) -> str | None:
    normalized = _optional_text(row, "normalized_value")
    if normalized is not None:
        return normalized
    return _optional_text(row, "raw_value")


def _normalize_st_text(raw_text: str) -> str:
    text = unicodedata.normalize("NFKC", raw_text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", text).strip()


def _db_value_type(row: Mapping[str, object]) -> str | None:
    value = _row_value(row, "raw_value_type")
    if value is None:
        return None
    return str(value).strip()


def _section_code(row: Mapping[str, object]) -> str | None:
    value = _optional_text(row, "section_code")
    if value is None:
        return None
    stripped = value.strip()
    if stripped == "":
        return None
    return stripped


def _optional_text(row: Mapping[str, object], column: str) -> str | None:
    value = _row_value(row, column)
    if value is None:
        return None
    return str(value)


def _required_row_text(row: Mapping[str, object], column: str) -> str:
    value = _optional_text(row, column)
    if value is None or value.strip() == "":
        raise ValueError(f"row column must not be empty: {column}")
    return value.strip()


def _require_columns(row: Mapping[str, object], columns: tuple[str, ...]) -> None:
    for column in columns:
        _row_value(row, column)


def _row_value(row: Mapping[str, object], column: str) -> object:
    if column not in row:
        raise ValueError(f"row missing required column: {column}")
    return row[column]
