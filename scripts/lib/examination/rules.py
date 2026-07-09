"""Presence and rule evaluation for Phase7 examination checks."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from .alternative import has_alternative_value
from .calculate import calculate
from .models import (
    REASON_CALCULATION_SOURCE_MISSING,
    REASON_MISSING,
    REASON_NOT_IMPLEMENTED,
    STATUS_ALTERNATIVE,
    STATUS_CALCULATED,
    STATUS_INVALID,
    STATUS_MISSING,
    STATUS_OK,
    ExamValue,
    ItemResult,
    MethodRule,
)


NOT_IMPLEMENTED_RULE_CODES = {"METABOLIC_SYNDROME", "HEALTH_GUIDANCE_LEVEL"}
NOT_IMPLEMENTED_IDENTITY_CODES = {"9N501", "9N506"}


@dataclass
class ValueIndex:
    values_by_identity: dict[str, list[ExamValue]]
    values_by_method: dict[str, list[ExamValue]]
    values_by_namecode: dict[str, list[ExamValue]]


def build_value_index(values: list[ExamValue]) -> ValueIndex:
    by_identity: dict[str, list[ExamValue]] = defaultdict(list)
    by_method: dict[str, list[ExamValue]] = defaultdict(list)
    by_namecode: dict[str, list[ExamValue]] = defaultdict(list)
    for value in values:
        if value.identity_code:
            by_identity[value.identity_code].append(value)
        if value.method_code:
            by_method[value.method_code].append(value)
        if value.namecode:
            by_namecode[value.namecode].append(value)
    return ValueIndex(by_identity, by_method, by_namecode)


def evaluate_identity(
    identity_code: str,
    *,
    rules: list[MethodRule],
    namecodes: set[str],
    index: ValueIndex,
    result_by_identity: dict[str, ItemResult],
) -> ItemResult:
    if identity_code in NOT_IMPLEMENTED_IDENTITY_CODES:
        return ItemResult(identity_code, STATUS_INVALID, f"{REASON_NOT_IMPLEMENTED}:{identity_code}")

    direct_values = index.values_by_identity.get(identity_code, [])
    if any(value.has_valid_value for value in direct_values):
        return ItemResult(identity_code, STATUS_OK)

    if not rules:
        if any(value.has_valid_value for namecode in namecodes for value in index.values_by_namecode.get(namecode, [])):
            return ItemResult(identity_code, STATUS_OK)
        return ItemResult(identity_code, STATUS_MISSING, REASON_MISSING)

    first_missing: ItemResult | None = None
    for rule in rules:
        result = evaluate_rule(rule, index=index, result_by_identity=result_by_identity)
        if result.is_ok_like or result.status == STATUS_INVALID:
            return ItemResult(identity_code, result.status, result.reason)
        first_missing = first_missing or result
    return first_missing or ItemResult(identity_code, STATUS_MISSING, REASON_MISSING)


def evaluate_rule(
    rule: MethodRule,
    *,
    index: ValueIndex,
    result_by_identity: dict[str, ItemResult],
) -> ItemResult:
    mode = rule.presence_value_mode or "ANY_VALID_VALUE"
    values_for_method = index.values_by_method.get(rule.method_code, [])
    if mode == "ANY_RECORD":
        if values_for_method:
            return ItemResult(rule.identity_code, STATUS_OK)
        return ItemResult(rule.identity_code, STATUS_MISSING, REASON_MISSING)
    if mode == "ANY_VALID_VALUE":
        if any(value.has_valid_value for value in values_for_method):
            return ItemResult(rule.identity_code, STATUS_OK)
        return ItemResult(rule.identity_code, STATUS_MISSING, REASON_MISSING)
    if mode == "ANY_OF_NAMECODES":
        if any(value.has_valid_value for namecode in rule.source_namecodes for value in index.values_by_namecode.get(namecode, [])):
            return ItemResult(rule.identity_code, STATUS_OK)
        return ItemResult(rule.identity_code, STATUS_MISSING, REASON_MISSING)
    if mode == "CALCULATED":
        return evaluate_calculated(rule, index=index, result_by_identity=result_by_identity)
    if mode == "ALTERNATIVE":
        return evaluate_alternative(rule, index=index, result_by_identity=result_by_identity)
    return ItemResult(rule.identity_code, STATUS_INVALID, f"UNKNOWN_PRESENCE_VALUE_MODE:{mode}")


def evaluate_calculated(
    rule: MethodRule,
    *,
    index: ValueIndex,
    result_by_identity: dict[str, ItemResult],
) -> ItemResult:
    if rule.rule_code in NOT_IMPLEMENTED_RULE_CODES:
        return ItemResult(rule.identity_code, STATUS_INVALID, f"{REASON_NOT_IMPLEMENTED}:{rule.identity_code}")
    if not rule.rule_code:
        return ItemResult(rule.identity_code, STATUS_MISSING, REASON_MISSING)

    sources: dict[str, Decimal] = {}
    for source_identity in rule.source_identity_codes:
        value = first_decimal_for_identity(source_identity, index, result_by_identity)
        if value is None:
            return ItemResult(rule.identity_code, STATUS_MISSING, REASON_CALCULATION_SOURCE_MISSING)
        sources[source_identity] = value
    try:
        calculate(rule.rule_code, sources)
    except NotImplementedError:
        return ItemResult(rule.identity_code, STATUS_INVALID, f"{REASON_NOT_IMPLEMENTED}:{rule.identity_code}")
    except Exception:
        return ItemResult(rule.identity_code, STATUS_INVALID, "CALCULATION_FAILED")
    return ItemResult(rule.identity_code, STATUS_CALCULATED)


def evaluate_alternative(
    rule: MethodRule,
    *,
    index: ValueIndex,
    result_by_identity: dict[str, ItemResult],
) -> ItemResult:
    for source_identity in rule.source_identity_codes:
        source_result = result_by_identity.get(source_identity)
        if source_result and source_result.is_ok_like:
            return ItemResult(rule.identity_code, STATUS_ALTERNATIVE)
    if has_alternative_value(
        values_by_identity=index.values_by_identity,
        values_by_method=index.values_by_method,
        values_by_namecode=index.values_by_namecode,
        source_identity_codes=rule.source_identity_codes,
        source_method_codes=rule.source_method_codes,
        source_namecodes=rule.source_namecodes,
    ):
        return ItemResult(rule.identity_code, STATUS_ALTERNATIVE)
    return ItemResult(rule.identity_code, STATUS_MISSING, REASON_MISSING)


def first_decimal_for_identity(
    identity_code: str,
    index: ValueIndex,
    result_by_identity: dict[str, ItemResult],
) -> Decimal | None:
    for value in index.values_by_identity.get(identity_code, []):
        if value.has_valid_value:
            decimal_value = value.decimal_value()
            if decimal_value is not None:
                return decimal_value
    result = result_by_identity.get(identity_code)
    if result and result.is_ok_like:
        # Calculated values are not persisted as numeric rows in Phase7; callers that
        # depend on derived numeric sources should be implemented explicitly later.
        return None
    return None
