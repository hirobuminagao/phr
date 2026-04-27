
from __future__ import annotations
import json

from dataclasses import dataclass
from typing import Any

from scripts.from_fund.script_lib.company_match_rules import apply_company_match_rule


@dataclass(frozen=True)
class CompanyMappingResult:
    mapped_employer_code: int | None
    mapped_department_code: int | None
    status: str
    reason: str
    mapping_id: int | None = None
    source_match_key: str | None = None


def _split_columns(columns: str | None) -> list[str]:
    if not columns:
        return []
    return [c.strip() for c in columns.split(",") if c.strip()]


def _values_from_row(row: dict[str, Any], columns: list[str]) -> list[Any]:
    return [row.get(c) for c in columns]



def _build_match_key(
    row: dict[str, Any],
    *,
    target_columns: str | None,
    match_rule: str | None,
) -> str | None:
    columns = _split_columns(target_columns)
    if not columns or not match_rule:
        return None
    values = _values_from_row(row, columns)
    return apply_company_match_rule(match_rule, values)


# --- source_match_conditions support ---

def _condition_value(row: dict[str, Any], column: str) -> str | None:
    value = row.get(column)
    if value is None:
        return None
    return str(value)


def _source_conditions_match(
    staging_row: dict[str, Any],
    conditions_json: Any,
) -> tuple[bool, str]:
    """source_match_conditions(JSON) を評価する。

    JSON例:
    [
      {"column":"insurance_symbol_norm","operator":"eq","value":"100"},
      {"column":"relationship_name_match","operator":"neq","value":"本人"}
    ]
    """
    if conditions_json in (None, ""):
        return True, "no source_match_conditions"

    try:
        conditions = json.loads(conditions_json) if isinstance(conditions_json, str) else conditions_json
    except json.JSONDecodeError as e:
        return False, f"invalid source_match_conditions json: {e}"

    if not isinstance(conditions, list):
        return False, "source_match_conditions must be a JSON array"

    for condition in conditions:
        if not isinstance(condition, dict):
            return False, "source_match_conditions item must be an object"

        column = condition.get("column")
        operator = condition.get("operator") or "eq"
        expected = condition.get("value")

        if not column:
            return False, "source_match_conditions item missing column"

        actual = _condition_value(staging_row, str(column))
        expected_str = None if expected is None else str(expected)

        if operator == "eq":
            if actual != expected_str:
                return False, f"condition failed: {column}={actual} != {expected_str}"
        elif operator == "neq":
            if actual == expected_str:
                return False, f"condition failed: {column}={actual} == {expected_str}"
        else:
            return False, f"unsupported condition operator: {operator}"

    return True, "source_match_conditions matched"


def _resolve_lookup_company_master(
    *,
    mapping: dict[str, Any],
    source_match_key: str | None,
    hia_company_rows: list[dict[str, Any]],
) -> CompanyMappingResult:
    mapping_id = mapping.get("fund_company_mapping_id")
    if not source_match_key:
        return CompanyMappingResult(
            None,
            None,
            "not_matched",
            "source_match_key is empty",
            mapping_id=mapping_id,
            source_match_key=source_match_key,
        )

    company_lookup_columns = mapping.get("company_lookup_columns")
    company_lookup_rule = mapping.get("company_lookup_rule")
    if not company_lookup_columns or not company_lookup_rule:
        return CompanyMappingResult(
            None,
            None,
            "config_error",
            "company_lookup_columns or company_lookup_rule is empty",
            mapping_id=mapping_id,
            source_match_key=source_match_key,
        )

    matched_rows: list[dict[str, Any]] = []
    for company_row in hia_company_rows:
        company_match_key = _build_match_key(
            company_row,
            target_columns=str(company_lookup_columns),
            match_rule=str(company_lookup_rule),
        )
        if company_match_key == source_match_key:
            matched_rows.append(company_row)

    if not matched_rows:
        return CompanyMappingResult(
            None,
            None,
            "not_found",
            f"no hia_company_master row for source_match_key={source_match_key}",
            mapping_id=mapping_id,
            source_match_key=source_match_key,
        )

    if len(matched_rows) > 1:
        return CompanyMappingResult(
            None,
            None,
            "multiple_match",
            f"multiple hia_company_master rows for source_match_key={source_match_key}",
            mapping_id=mapping_id,
            source_match_key=source_match_key,
        )

    row = matched_rows[0]
    return CompanyMappingResult(
        row.get("employer_code"),
        row.get("department_code"),
        "mapped",
        "lookup_company_master matched",
        mapping_id=mapping_id,
        source_match_key=source_match_key,
    )


def _resolve_fixed(
    *,
    mapping: dict[str, Any],
    source_match_key: str | None,
) -> CompanyMappingResult:
    mapping_id = mapping.get("fund_company_mapping_id")
    expected_key = mapping.get("source_match_key")
    if expected_key is not None and str(expected_key) != str(source_match_key):
        return CompanyMappingResult(
            None,
            None,
            "not_matched",
            f"source_match_key mismatch: expected={expected_key}, actual={source_match_key}",
            mapping_id=mapping_id,
            source_match_key=source_match_key,
        )

    return CompanyMappingResult(
        mapping.get("fixed_employer_code"),
        mapping.get("fixed_department_code"),
        "mapped",
        "fixed mapping matched",
        mapping_id=mapping_id,
        source_match_key=source_match_key,
    )


def resolve_company_mapping(
    *,
    staging_row: dict[str, Any],
    mappings: list[dict[str, Any]],
    hia_company_rows: list[dict[str, Any]],
) -> CompanyMappingResult:
    """1件のstaging行に対して会社・部署mappingを解決する。"""
    if not mappings:
        return CompanyMappingResult(None, None, "not_found", "mapping rules are empty")

    for mapping in mappings:
        source_match_key = _build_match_key(
            staging_row,
            target_columns=mapping.get("source_target_columns"),
            match_rule=mapping.get("source_match_rule"),
        )
        source_match_operator = mapping.get("source_match_operator") or "eq"

        conditions_ok, conditions_reason = _source_conditions_match(
            staging_row,
            mapping.get("source_match_conditions"),
        )
        if not conditions_ok:
            result = CompanyMappingResult(
                None,
                None,
                "not_matched",
                conditions_reason,
                mapping_id=mapping.get("fund_company_mapping_id"),
                source_match_key=source_match_key,
            )
            continue

        mapping_type = mapping.get("mapping_type")
        if mapping_type == "lookup_company_master":
            result = _resolve_lookup_company_master(
                mapping=mapping,
                source_match_key=source_match_key,
                hia_company_rows=hia_company_rows,
            )
        elif mapping_type == "fixed":
            if source_match_operator == "neq":
                expected_key = mapping.get("source_match_key")
                if expected_key is None or str(source_match_key) != str(expected_key):
                    result = CompanyMappingResult(
                        mapping.get("fixed_employer_code"),
                        mapping.get("fixed_department_code"),
                        "mapped",
                        "fixed mapping matched (neq)",
                        mapping_id=mapping.get("fund_company_mapping_id"),
                        source_match_key=source_match_key,
                    )
                else:
                    result = CompanyMappingResult(
                        None,
                        None,
                        "not_matched",
                        f"neq condition failed: {source_match_key} == {expected_key}",
                        mapping_id=mapping.get("fund_company_mapping_id"),
                        source_match_key=source_match_key,
                    )
            else:
                result = _resolve_fixed(
                    mapping=mapping,
                    source_match_key=source_match_key,
                )
        else:
            result = CompanyMappingResult(
                None,
                None,
                "config_error",
                f"unsupported mapping_type={mapping_type}",
                mapping_id=mapping.get("fund_company_mapping_id"),
                source_match_key=source_match_key,
            )

        if result.status == "mapped":
            return result

    return CompanyMappingResult(None, None, "not_found", "no mapping rule matched")