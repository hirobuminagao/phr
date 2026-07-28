"""Lookup helpers for CSV exam result format and mapping rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from scripts.lib.csv.csv_loader import CsvHeaderColumn, CsvLoadResult
from scripts.lib.db.schemas import PHR_MASTER


@dataclass(frozen=True)
class CsvMappingCondition:
    condition_id: int
    rule_id: int
    condition_group_no: int
    condition_type: str
    locator_type: str | None
    header_context: str | None
    header_name: str | None
    header_occurrence: int | None
    column_no: int | None
    operator: str | None
    expected_value: str | None
    source_role: str | None
    priority: int
    resolved_column_no: int | None = None


@dataclass(frozen=True)
class CsvMappingRule:
    rule_id: int
    csv_format_version_id: int
    rule_key: str
    target_kind: str
    target_resolution_type: str | None
    selection_mode: str
    selection_group_code: str | None
    target_namecode: str | None
    target_identity_item_code: str | None
    target_field: str | None
    method_structure_type: str | None
    value_source_type: str
    fixed_value: str | None
    value_join_separator: str | None
    raw_value_type: str | None
    raw_unit: str | None
    is_required: bool
    priority: int
    conditions: tuple[CsvMappingCondition, ...]


def _compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def find_csv_format_version(
    cur: Any,
    *,
    exam_facility_id: int,
    header_sha256: str,
    exam_date: date | None = None,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Find the active CSV format version for a facility/header fingerprint."""

    params: list[Any] = [exam_facility_id, header_sha256]
    validity_sql = ""
    if exam_date is not None:
        validity_sql = """
          AND (valid_from IS NULL OR valid_from <= %s)
          AND (valid_to IS NULL OR valid_to >= %s)
        """
        params.extend([exam_date, exam_date])

    cur.execute(
        f"""
        SELECT *
        FROM `{master_db}`.`csv_format_versions`
        WHERE exam_facility_id = %s
          AND header_sha256 = %s
          AND is_active = 1
          {validity_sql}
        ORDER BY valid_from DESC, csv_format_version_id DESC
        LIMIT 1
        """,
        tuple(params),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def find_csv_format_versions_by_header(
    cur: Any,
    *,
    exam_facility_id: int,
    header_sha256: str,
    exam_date: date | None = None,
    master_db: str = PHR_MASTER,
) -> list[dict[str, Any]]:
    """Find all active CSV format versions for a facility/header fingerprint."""

    params: list[Any] = [exam_facility_id, header_sha256]
    validity_sql = ""
    if exam_date is not None:
        validity_sql = """
          AND (valid_from IS NULL OR valid_from <= %s)
          AND (valid_to IS NULL OR valid_to >= %s)
        """
        params.extend([exam_date, exam_date])

    cur.execute(
        f"""
        SELECT *
        FROM `{master_db}`.`csv_format_versions`
        WHERE exam_facility_id = %s
          AND header_sha256 = %s
          AND is_active = 1
          {validity_sql}
        ORDER BY is_default_for_facility DESC, valid_from DESC, csv_format_version_id DESC
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def get_csv_format_version_by_id(
    cur: Any,
    csv_format_version_id: int,
    *,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM `{master_db}`.`csv_format_versions`
        WHERE csv_format_version_id = %s
          AND is_active = 1
        LIMIT 1
        """,
        (csv_format_version_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _fetch_rule_rows(cur: Any, *, csv_format_version_id: int, master_db: str) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM `{master_db}`.`csv_exam_result_mapping_rules`
        WHERE csv_format_version_id = %s
          AND is_active = 1
        ORDER BY priority, csv_exam_result_mapping_rule_id
        """,
        (csv_format_version_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def _fetch_condition_rows(cur: Any, *, rule_ids: list[int], master_db: str) -> list[dict[str, Any]]:
    if not rule_ids:
        return []
    placeholders = ", ".join(["%s"] * len(rule_ids))
    cur.execute(
        f"""
        SELECT *
        FROM `{master_db}`.`csv_exam_result_mapping_conditions`
        WHERE csv_exam_result_mapping_rule_id IN ({placeholders})
          AND is_active = 1
        ORDER BY csv_exam_result_mapping_rule_id, condition_group_no, priority,
                 csv_exam_result_mapping_condition_id
        """,
        tuple(rule_ids),
    )
    return [dict(row) for row in cur.fetchall()]


def _match_header_column(
    condition: dict[str, Any],
    columns: list[CsvHeaderColumn],
) -> CsvHeaderColumn | None:
    locator_type = _compact_text(condition.get("locator_type"))
    if locator_type == "COLUMN_NO" and condition.get("column_no") is not None:
        column_no = int(condition["column_no"])
        return next((column for column in columns if column.column_no == column_no), None)

    if locator_type not in {None, "HEADER_NAME", "HEADER_CONTEXT_AND_NAME"}:
        return None

    expected_context = _compact_text(condition.get("header_context"))
    expected_name = _compact_text(condition.get("header_name"))
    expected_occurrence = condition.get("header_occurrence")

    matches = [
        column
        for column in columns
        if (expected_name is None or column.header_name == expected_name)
        and (expected_context is None or column.context == expected_context)
    ]
    if expected_occurrence is not None:
        occurrence = int(expected_occurrence)
        matches = [column for column in matches if column.occurrence == occurrence]

    if len(matches) == 1:
        return matches[0]
    return None


def _build_condition(row: dict[str, Any], csv_result: CsvLoadResult | None) -> CsvMappingCondition:
    resolved_column_no: int | None = None
    if csv_result is not None:
        matched = _match_header_column(row, csv_result.header_set.normalized_columns)
        resolved_column_no = matched.column_no if matched else None

    return CsvMappingCondition(
        condition_id=int(row["csv_exam_result_mapping_condition_id"]),
        rule_id=int(row["csv_exam_result_mapping_rule_id"]),
        condition_group_no=int(row["condition_group_no"]),
        condition_type=str(row["condition_type"]),
        locator_type=_compact_text(row.get("locator_type")),
        header_context=_compact_text(row.get("header_context")),
        header_name=_compact_text(row.get("header_name")),
        header_occurrence=int(row["header_occurrence"]) if row.get("header_occurrence") is not None else None,
        column_no=int(row["column_no"]) if row.get("column_no") is not None else None,
        operator=_compact_text(row.get("operator")),
        expected_value=_compact_text(row.get("expected_value")),
        source_role=_compact_text(row.get("source_role")),
        priority=int(row["priority"]),
        resolved_column_no=resolved_column_no,
    )


def load_csv_mapping_rules(
    cur: Any,
    *,
    csv_format_version_id: int,
    csv_result: CsvLoadResult | None = None,
    master_db: str = PHR_MASTER,
) -> list[CsvMappingRule]:
    """Load active mapping rules, optionally resolving header conditions to columns."""

    rule_rows = _fetch_rule_rows(cur, csv_format_version_id=csv_format_version_id, master_db=master_db)
    condition_rows = _fetch_condition_rows(
        cur,
        rule_ids=[int(row["csv_exam_result_mapping_rule_id"]) for row in rule_rows],
        master_db=master_db,
    )

    conditions_by_rule: dict[int, list[CsvMappingCondition]] = {}
    for row in condition_rows:
        condition = _build_condition(row, csv_result)
        conditions_by_rule.setdefault(condition.rule_id, []).append(condition)

    rules: list[CsvMappingRule] = []
    for row in rule_rows:
        rule_id = int(row["csv_exam_result_mapping_rule_id"])
        rules.append(
            CsvMappingRule(
                rule_id=rule_id,
                csv_format_version_id=int(row["csv_format_version_id"]),
                rule_key=str(row["rule_key"]),
                target_kind=str(row["target_kind"]),
                target_resolution_type=_compact_text(row.get("target_resolution_type")),
                selection_mode=str(row["selection_mode"]),
                selection_group_code=_compact_text(row.get("selection_group_code")),
                target_namecode=_compact_text(row.get("target_namecode")),
                target_identity_item_code=_compact_text(row.get("target_identity_item_code")),
                target_field=_compact_text(row.get("target_field")),
                method_structure_type=_compact_text(row.get("method_structure_type")),
                value_source_type=_compact_text(row.get("value_source_type")) or "SOURCE",
                fixed_value=_compact_text(row.get("fixed_value")),
                value_join_separator=(
                    str(row["value_join_separator"])
                    if row.get("value_join_separator") is not None
                    else None
                ),
                raw_value_type=_compact_text(row.get("raw_value_type")),
                raw_unit=_compact_text(row.get("raw_unit")),
                is_required=bool(row.get("is_required")),
                priority=int(row["priority"]),
                conditions=tuple(conditions_by_rule.get(rule_id, [])),
            )
        )

    return rules
