"""Specific health examination checks for MHLW XML output readiness."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from scripts.from_medical.script_lib.article44_models import (
    CDValue,
    CheckResult,
    ExpectedValueType,
    RequiredNamecode,
    STValue,
    ValueInvalidReason,
    ValueMap,
    ValueState,
)
from scripts.lib.examination.models import RESULT_NG, RESULT_OK, STATUS_INVALID, STATUS_MISSING, STATUS_OK
from scripts.lib.examination.report_classification import calculate_full_age


SPECIFIC_TARGET_MIN_AGE = 40
SPECIFIC_TARGET_MAX_AGE = 74

SPECIFIC_REQUIRED_NAMECODES: tuple[RequiredNamecode, ...] = (
    RequiredNamecode("9N141000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N501000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N506000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N511000000000049", ExpectedValueType.ST),
    RequiredNamecode("9N516000000000049", ExpectedValueType.ST),
    RequiredNamecode("9N701000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N706000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N711000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N716000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N721000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N726000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N731000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N736000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N741000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N746000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N751000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N756000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N872000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N766000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N771000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N782000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N781000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N786000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N791000000000011", ExpectedValueType.CO),
    RequiredNamecode("9N796000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N801000000000011", ExpectedValueType.CD),
    RequiredNamecode("9N808000000000011", ExpectedValueType.CD),
)

SPECIFIC_ITEM_NAMES: dict[str, str] = {
    "9N141000000000011": "採血時間（食後）",
    "9N501000000000011": "メタボリックシンドローム判定",
    "9N506000000000011": "保健指導レベル",
    "9N511000000000049": "医師の診断（判定）",
    "9N516000000000049": "医師名",
    "9N701000000000011": "服薬1（血圧）",
    "9N706000000000011": "服薬2（血糖）",
    "9N711000000000011": "服薬3（脂質）",
    "9N716000000000011": "既往歴1（脳血管）",
    "9N721000000000011": "既往歴2（心血管）",
    "9N726000000000011": "既往歴3（腎不全・人工透析）",
    "9N731000000000011": "貧血",
    "9N736000000000011": "喫煙",
    "9N741000000000011": "20歳からの体重変化",
    "9N746000000000011": "30分以上の運動習慣",
    "9N751000000000011": "歩行又は身体活動",
    "9N756000000000011": "歩行速度",
    "9N872000000000011": "咀嚼",
    "9N766000000000011": "食べ方1（早食い等）",
    "9N771000000000011": "食べ方2（就寝前）",
    "9N782000000000011": "食べ方3（間食）",
    "9N781000000000011": "食習慣",
    "9N786000000000011": "飲酒",
    "9N791000000000011": "飲酒量",
    "9N796000000000011": "睡眠",
    "9N801000000000011": "生活習慣の改善",
    "9N808000000000011": "特定保健指導の受診歴",
}


def aggregate_specific_result(
    *,
    value_map: ValueMap,
    birthdate: Any,
    age_reference_date: date | None,
    legal_result: str,
) -> tuple[str, str | None]:
    """Return specific health check result and reason summary.

    Article 44 overlap items are treated as satisfied when the legal check is OK.
    This v0 therefore checks only specific-health-only items that commonly break
    HIA/MHLW XML acceptance.
    """

    target = specific_target_state(birthdate=birthdate, age_reference_date=age_reference_date)
    if target.status != STATUS_OK:
        return RESULT_NG, f"AGE:{target.reason or target.status}"
    if target.reason == "NOT_TARGET_AGE":
        return RESULT_OK, "対象外:年度末年齢が40-74歳ではありません"
    reasons: list[str] = []
    if legal_result != RESULT_OK:
        reasons.append("法定重複項目:LEGAL_CHECK_NOT_OK")
    for required in SPECIFIC_REQUIRED_NAMECODES:
        result = check_required_specific_value(value_map, required.namecode)
        if result.status == STATUS_OK:
            continue
        item_name = SPECIFIC_ITEM_NAMES.get(required.namecode, required.namecode)
        reasons.append(f"{required.namecode}:{item_name}:{result.reason or result.status}")
    if reasons:
        return RESULT_NG, " | ".join(reasons)
    return RESULT_OK, None


def specific_target_state(*, birthdate: Any, age_reference_date: date | None) -> CheckResult:
    birth = parse_date_value(birthdate)
    if birth is None:
        return CheckResult(STATUS_INVALID, "BIRTHDATE_MISSING")
    if age_reference_date is None:
        return CheckResult(STATUS_INVALID, "AGE_REFERENCE_DATE_MISSING")
    age = calculate_full_age(birth, age_reference_date)
    if SPECIFIC_TARGET_MIN_AGE <= age <= SPECIFIC_TARGET_MAX_AGE:
        return CheckResult(STATUS_OK, f"TARGET_AGE:{age}")
    return CheckResult(STATUS_OK, "NOT_TARGET_AGE")


def check_required_specific_value(value_map: ValueMap, namecode: str) -> CheckResult:
    value = value_map[namecode]
    if value.duplicate_count:
        return CheckResult(
            STATUS_INVALID,
            f"{ValueInvalidReason.DUPLICATE_NAMECODE.value}:count={value.duplicate_count}",
        )
    if value.invalid_reason:
        return CheckResult(STATUS_INVALID, value.invalid_reason.value)
    if value.value_state in {ValueState.NOT_FOUND, ValueState.NULL, ValueState.EMPTY}:
        return CheckResult(STATUS_MISSING, value.value_state.value)
    if isinstance(value, CDValue):
        if value.is_valid and value.code_value:
            return CheckResult(STATUS_OK)
        return CheckResult(STATUS_INVALID, "CODE_VALUE_MISSING")
    if isinstance(value, STValue):
        if value.is_valid and value.text and value.text.strip():
            return CheckResult(STATUS_OK)
        return CheckResult(STATUS_INVALID, "TEXT_VALUE_MISSING")
    if value.is_valid:
        return CheckResult(STATUS_OK)
    return CheckResult(STATUS_INVALID, "INVALID_VALUE")


def parse_date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None
