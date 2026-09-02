"""Apply CSV exam result mapping rules to one parsed CSV row."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scripts.lib.db.lookup.csv_exam_result_mapping import CsvMappingCondition, CsvMappingRule


SOURCE_COLUMN_CONDITION_TYPES = {"HEADER_MATCH", "SOURCE_COLUMN"}


@dataclass(frozen=True)
class ExtractedCsvRuleValue:
    rule: CsvMappingRule
    values_by_role: dict[str, str | None]
    matched_condition_group_no: int | None
    errors: tuple[str, ...] = field(default_factory=tuple)


def _cell_value(row: list[str], column_no: int | None) -> str | None:
    if column_no is None:
        return None
    index = column_no - 1
    if index < 0 or index >= len(row):
        return None
    return row[index]


def _compare(value: str | None, operator: str | None, expected: str | None) -> bool:
    op = (operator or "PRESENT").upper()
    if op == "PRESENT":
        return value is not None
    if op == "NOT_EMPTY":
        return value is not None and value != ""
    if op == "EMPTY":
        return value is None or value == ""
    if op == "EQUALS":
        return (value or "") == (expected or "")
    if op == "NOT_EQUALS":
        return (value or "") != (expected or "")
    if op == "IN":
        expected_values = {part.strip() for part in (expected or "").split(",")}
        return (value or "") in expected_values
    return False


def _condition_matches(condition: CsvMappingCondition, row: list[str]) -> bool:
    if condition.condition_type in SOURCE_COLUMN_CONDITION_TYPES:
        return (condition.resolved_column_no or condition.column_no) is not None
    if condition.condition_type == "CELL_VALUE":
        value = _cell_value(row, condition.resolved_column_no or condition.column_no)
        return _compare(value, condition.operator, condition.expected_value)
    return False


def _group_conditions(rule: CsvMappingRule) -> dict[int, list[CsvMappingCondition]]:
    groups: dict[int, list[CsvMappingCondition]] = {}
    for condition in rule.conditions:
        groups.setdefault(condition.condition_group_no, []).append(condition)
    return groups


def _extract_role_values(
    conditions: list[CsvMappingCondition],
    row: list[str],
    rule: CsvMappingRule,
) -> tuple[dict[str, str | None], list[str]]:
    source_values: dict[str, list[str | None]] = {}
    for condition in conditions:
        if condition.condition_type not in SOURCE_COLUMN_CONDITION_TYPES:
            continue
        role = condition.source_role or "VALUE"
        source_values.setdefault(role, []).append(
            _cell_value(row, condition.resolved_column_no or condition.column_no)
        )

    values_by_role: dict[str, str | None] = {}
    for role, values in source_values.items():
        if role == "VALUE" and len(values) > 1:
            separator = rule.value_join_separator
            if separator is None:
                return {}, ["MULTIPLE_VALUE_SOURCES_REQUIRE_JOIN_SEPARATOR"]
            exclude_values = set(rule.value_exclude_values)
            nonempty = [
                value
                for value in values
                if value not in {None, ""} and value not in exclude_values
            ]
            values_by_role[role] = separator.join(nonempty) if nonempty else None
        else:
            values_by_role[role] = values[0]

    if rule.value_source_type == "FIXED":
        if rule.fixed_value is None:
            return {}, ["FIXED_VALUE_NOT_CONFIGURED"]
        values_by_role["VALUE"] = rule.fixed_value
    elif rule.value_source_type != "SOURCE":
        return {}, [f"UNSUPPORTED_VALUE_SOURCE_TYPE:{rule.value_source_type}"]

    return values_by_role, []


def extract_rule_value(row: list[str], rule: CsvMappingRule) -> ExtractedCsvRuleValue:
    """Extract one rule's role values from a CSV row.

    Conditions in the same group are AND. Multiple groups are OR, and the first
    matching group by condition_group_no/priority is used.
    """

    groups = _group_conditions(rule)
    if not groups:
        return ExtractedCsvRuleValue(
            rule=rule,
            values_by_role={},
            matched_condition_group_no=None,
            errors=("RULE_HAS_NO_CONDITIONS",),
        )

    errors: list[str] = []
    for group_no in sorted(groups):
        conditions = groups[group_no]
        unresolved = [
            condition
            for condition in conditions
            if condition.locator_type in {"HEADER_NAME", "HEADER_CONTEXT_AND_NAME", "COLUMN_NO"}
            and (condition.resolved_column_no or condition.column_no) is None
        ]
        if unresolved:
            errors.append(f"UNRESOLVED_CONDITION_COLUMN:group={group_no}")
            continue

        if not all(_condition_matches(condition, row) for condition in conditions):
            continue

        values_by_role, value_errors = _extract_role_values(conditions, row, rule)

        return ExtractedCsvRuleValue(
            rule=rule,
            values_by_role=values_by_role,
            matched_condition_group_no=group_no,
            errors=tuple(errors + value_errors),
        )

    return ExtractedCsvRuleValue(
        rule=rule,
        values_by_role={},
        matched_condition_group_no=None,
        errors=tuple(errors + ["NO_CONDITION_GROUP_MATCHED"]),
    )


def extract_row_values(row: list[str], rules: list[CsvMappingRule]) -> list[ExtractedCsvRuleValue]:
    return [extract_rule_value(row, rule) for rule in rules]
