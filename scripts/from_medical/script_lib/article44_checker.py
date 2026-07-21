"""Article 44 item checkers for normalized examination values."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import cast

from scripts.from_medical.script_lib.article44_models import (
    Article44Result,
    CDValue,
    CheckResult,
    PQValue,
    STValue,
    ValueInvalidReason,
    ValueMap,
)
from scripts.lib.examination.check.all import has_all_valid
from scripts.lib.examination.check.any import has_any_valid
from scripts.lib.examination.check.compare import is_less_than
from scripts.lib.examination.check.finding import has_text
from scripts.lib.examination.models import (
    REASON_MISSING,
    STATUS_ALTERNATIVE,
    STATUS_INVALID,
    STATUS_MISSING,
    STATUS_OK,
)


Checker = Callable[[ValueMap], CheckResult]
Article44Value = PQValue | CDValue | STValue


CODE_FINDING_PRESENT = "1"
CODE_FINDING_ABSENT = "2"
CODE_FINDING_INDETERMINATE = "3"

NC_MEDICAL_HISTORY_FINDING = "9N056160400000049"
NC_MEDICAL_HISTORY_FINDING_FLAG = "9N056000000000011"
NC_SUBJECTIVE_SYMPTOMS_FINDING = "9N061160800000049"
NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG = "9N061000000000011"
NC_OBJECTIVE_SYMPTOMS_FINDING = "9N066160800000049"
NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG = "9N066000000000011"
NC_HEIGHT = "9N001000000000001"
NC_WEIGHT = "9N006000000000001"
NC_WAIST_MEASURED = "9N016160100000001"
NC_WAIST_SELF_MEASURED = "9N016160200000001"
NC_WAIST_SELF_REPORTED = "9N016160300000001"
NC_BMI = "9N011000000000001"
NC_RIGHT_VISION_UNCORRECTED = "9E160162100000001"
NC_LEFT_VISION_UNCORRECTED = "9E160162200000001"
NC_RIGHT_VISION_CORRECTED = "9E160162500000001"
NC_LEFT_VISION_CORRECTED = "9E160162600000001"
NC_HEARING_RIGHT_1000HZ = "9D100163100000011"
NC_HEARING_CONVERSATION = "9D100160900000049"
NC_HEARING_LEFT_1000HZ = "9D100163200000011"
NC_HEARING_RIGHT_4000HZ = "9D100163500000011"
NC_HEARING_LEFT_4000HZ = "9D100163600000011"
NC_CHEST_XRAY_RESULT_1 = "9N201000000000011"
NC_CHEST_XRAY_FINDING_FLAG_1 = "9N206160700000011"
NC_CHEST_XRAY_FINDING_1 = "9N206160800000049"
NC_CHEST_XRAY_FINDING_FLAG_2 = "9N221160700000011"
NC_CHEST_XRAY_FINDING_2 = "9N221160800000049"
NC_CHEST_XRAY_RESULT_2 = "9N216000000000011"
NC_SYSTOLIC_BLOOD_PRESSURE_REPRESENTATIVE = "9A755000000000001"
NC_SYSTOLIC_BLOOD_PRESSURE_SECOND = "9A752000000000001"
NC_SYSTOLIC_BLOOD_PRESSURE_FIRST = "9A751000000000001"
NC_DIASTOLIC_BLOOD_PRESSURE_REPRESENTATIVE = "9A765000000000001"
NC_DIASTOLIC_BLOOD_PRESSURE_SECOND = "9A762000000000001"
NC_DIASTOLIC_BLOOD_PRESSURE_FIRST = "9A761000000000001"
NC_HEMOGLOBIN = "2A030000001930101"
NC_RED_BLOOD_CELL_COUNT = "2A020000001930101"
NC_AST_JSCC = "3B035000002327201"
NC_AST_OTHER = "3B035000002399901"
NC_ALT_JSCC = "3B045000002327201"
NC_ALT_OTHER = "3B045000002399901"
NC_GAMMA_GT_JSCC = "3B090000002327101"
NC_GAMMA_GT_OTHER = "3B090000002399901"
NC_LDL_DIRECT = "3F077000002327101"
NC_LDL_OTHER_1 = "3F077000002327201"
NC_LDL_OTHER_2 = "3F077000002399901"
NC_HDL_JSCC = "3F070000002327101"
NC_HDL_OTHER_1 = "3F070000002327201"
NC_HDL_OTHER_2 = "3F070000002399901"
NC_TRIGLYCERIDES_FASTING_JSCC = "3F015000002327101"
NC_TRIGLYCERIDES_FASTING_OTHER_1 = "3F015000002327201"
NC_TRIGLYCERIDES_FASTING_OTHER_2 = "3F015000002399901"
NC_TRIGLYCERIDES_CASUAL_JSCC = "3F015129902327101"
NC_TRIGLYCERIDES_CASUAL_OTHER_1 = "3F015129902327201"
NC_TRIGLYCERIDES_CASUAL_OTHER_2 = "3F015129902399901"
NC_FASTING_GLUCOSE_1 = "3D010000001926101"
NC_FASTING_GLUCOSE_2 = "3D010000002227101"
NC_FASTING_GLUCOSE_3 = "3D010000001927201"
NC_FASTING_GLUCOSE_4 = "3D010000001999901"
NC_HBA1C_1 = "3D046000001906202"
NC_HBA1C_2 = "3D046000001920402"
NC_HBA1C_3 = "3D046000001927102"
NC_HBA1C_4 = "3D046000001999902"
NC_CASUAL_GLUCOSE_1 = "3D010129901926101"
NC_CASUAL_GLUCOSE_2 = "3D010129902227101"
NC_CASUAL_GLUCOSE_3 = "3D010129901927201"
NC_CASUAL_GLUCOSE_4 = "3D010129901999901"
NC_BLOOD_SAMPLING_TIME = "9N141000000000011"
NC_URINE_GLUCOSE_1 = "1A020000000191111"
NC_URINE_GLUCOSE_2 = "1A020000000190111"
NC_URINE_PROTEIN_1 = "1A010000000191111"
NC_URINE_PROTEIN_2 = "1A010000000190111"
NC_ELECTROCARDIOGRAM_FINDING_FLAG = "9A110160700000011"
NC_ELECTROCARDIOGRAM_FINDING = "9A110160800000049"


def _invalid_reason_text(value: Article44Value) -> str | None:
    invalid_reason = value.invalid_reason
    if invalid_reason is None:
        return None
    return invalid_reason.value


def _has_duplicate(values: Iterable[Article44Value]) -> Article44Value | None:
    for value in values:
        if value.invalid_reason == ValueInvalidReason.DUPLICATE_NAMECODE:
            return value
    return None


def _duplicate_reason(value: Article44Value) -> str:
    duplicate_count = value.duplicate_count
    if duplicate_count is None:
        return ValueInvalidReason.DUPLICATE_NAMECODE.value
    return f"{ValueInvalidReason.DUPLICATE_NAMECODE.value}:count={duplicate_count}"


def _missing_or_invalid(values: Iterable[Article44Value]) -> CheckResult:
    values = tuple(values)
    duplicate = _has_duplicate(values)
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))

    for value in values:
        reason = _invalid_reason_text(value)
        if reason is not None:
            return CheckResult(status=STATUS_INVALID, reason=reason)

    return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)


def _any_valid(value_map: ValueMap, namecodes: tuple[str, ...]) -> CheckResult:
    values = tuple(value_map[namecode] for namecode in namecodes)
    duplicate = _has_duplicate(values)
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    if has_any_valid(values):
        return CheckResult(status=STATUS_OK)
    return _missing_or_invalid(values)


def _finding_result(value_map: ValueMap, flag_namecode: str, text_namecode: str) -> CheckResult:
    flag_value = cast(CDValue, value_map[flag_namecode])
    text_value = cast(STValue, value_map[text_namecode])
    duplicate = _has_duplicate((flag_value, text_value))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))

    if not flag_value.is_valid:
        return _missing_or_invalid((flag_value,))

    if flag_value.code_value == CODE_FINDING_ABSENT:
        return CheckResult(status=STATUS_OK)
    if flag_value.code_value != CODE_FINDING_PRESENT:
        return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)

    if has_text(text_value):
        return CheckResult(status=STATUS_OK)
    return _missing_or_invalid((text_value,))


def _blood_sampling_time_allows_casual_glucose(value: CDValue) -> bool:
    return value.is_valid and value.code_value in {"2", "3"}


def _alternative_reason(namecode: str) -> str:
    return f"ALTERNATIVE:{namecode}"


def _chest_xray_finding_pattern(
    flag_value: CDValue,
    text_value: STValue,
    alternative_namecode: str,
) -> CheckResult:
    duplicate = _has_duplicate((flag_value, text_value))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))

    if not flag_value.is_valid:
        return _missing_or_invalid((flag_value,))

    if flag_value.code_value in {CODE_FINDING_ABSENT, CODE_FINDING_INDETERMINATE}:
        return CheckResult(
            status=STATUS_ALTERNATIVE,
            reason=_alternative_reason(alternative_namecode),
        )
    if flag_value.code_value != CODE_FINDING_PRESENT:
        return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)

    if has_text(text_value):
        return CheckResult(
            status=STATUS_ALTERNATIVE,
            reason=_alternative_reason(alternative_namecode),
        )
    return CheckResult(status=STATUS_INVALID, reason="FINDING_TEXT_MISSING")


def check_4401001001_medical_history(value_map: ValueMap) -> CheckResult:
    """Check medical history finding flag and text."""

    return _finding_result(
        value_map,
        NC_MEDICAL_HISTORY_FINDING_FLAG,
        NC_MEDICAL_HISTORY_FINDING,
    )


def check_4402001001_subjective_symptoms(value_map: ValueMap) -> CheckResult:
    """Check subjective symptoms finding flag and text."""

    return _finding_result(
        value_map,
        NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
        NC_SUBJECTIVE_SYMPTOMS_FINDING,
    )


def check_4402001002_objective_symptoms(value_map: ValueMap) -> CheckResult:
    """Check objective symptoms finding flag and text."""

    return _finding_result(
        value_map,
        NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
        NC_OBJECTIVE_SYMPTOMS_FINDING,
    )


def check_4403001001_height(value_map: ValueMap) -> CheckResult:
    """Check that height has a valid value."""

    return _any_valid(value_map, (NC_HEIGHT,))


def check_4403002001_weight(value_map: ValueMap) -> CheckResult:
    """Check that weight has a valid value."""

    return _any_valid(value_map, (NC_WEIGHT,))


def check_4403003001_waist(value_map: ValueMap) -> CheckResult:
    """Check waist measurement by priority and BMI-conditioned self report."""

    measured = cast(PQValue, value_map[NC_WAIST_MEASURED])
    self_measured = cast(PQValue, value_map[NC_WAIST_SELF_MEASURED])
    self_reported = cast(PQValue, value_map[NC_WAIST_SELF_REPORTED])
    bmi = cast(PQValue, value_map[NC_BMI])
    duplicate = _has_duplicate((measured, self_measured, self_reported, bmi))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))

    if measured.is_valid or self_measured.is_valid:
        return CheckResult(status=STATUS_OK)
    if not self_reported.is_valid:
        return _missing_or_invalid((measured, self_measured, self_reported))
    if is_less_than(bmi, Decimal("22")):
        return CheckResult(status=STATUS_OK)
    if _invalid_reason_text(bmi) is not None:
        return _missing_or_invalid((bmi,))
    return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)


def check_4403004001_vision(value_map: ValueMap) -> CheckResult:
    """Check that both right and left vision have valid values."""

    right = (
        value_map[NC_RIGHT_VISION_UNCORRECTED],
        value_map[NC_RIGHT_VISION_CORRECTED],
    )
    left = (
        value_map[NC_LEFT_VISION_UNCORRECTED],
        value_map[NC_LEFT_VISION_CORRECTED],
    )
    duplicate = _has_duplicate((*right, *left))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    if has_any_valid(right) and has_any_valid(left):
        return CheckResult(status=STATUS_OK)
    return _missing_or_invalid((*right, *left))


def check_4403005001_hearing(value_map: ValueMap) -> CheckResult:
    """Check hearing values."""

    standard_values = (
        value_map[NC_HEARING_RIGHT_1000HZ],
        value_map[NC_HEARING_LEFT_1000HZ],
        value_map[NC_HEARING_RIGHT_4000HZ],
        value_map[NC_HEARING_LEFT_4000HZ],
    )
    duplicate = _has_duplicate(standard_values)
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    for value in standard_values:
        reason = _invalid_reason_text(value)
        if reason is not None:
            return CheckResult(status=STATUS_INVALID, reason=reason)
    if has_all_valid(standard_values):
        return CheckResult(status=STATUS_OK)

    conversation = cast(STValue, value_map[NC_HEARING_CONVERSATION])
    duplicate = _has_duplicate((conversation,))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    reason = _invalid_reason_text(conversation)
    if reason is not None:
        return CheckResult(status=STATUS_INVALID, reason=reason)
    if conversation.is_valid:
        return CheckResult(
            status=STATUS_ALTERNATIVE,
            reason=_alternative_reason(NC_HEARING_CONVERSATION),
        )
    return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)


def check_4404001001_chest_xray(value_map: ValueMap) -> CheckResult:
    """Check chest X-ray result or finding patterns."""

    standard_results = (
        value_map[NC_CHEST_XRAY_RESULT_1],
        value_map[NC_CHEST_XRAY_RESULT_2],
    )
    duplicate = _has_duplicate(standard_results)
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    for value in standard_results:
        reason = _invalid_reason_text(value)
        if reason is not None:
            return CheckResult(status=STATUS_INVALID, reason=reason)
    if has_any_valid(standard_results):
        return CheckResult(status=STATUS_OK)

    pattern1 = _chest_xray_finding_pattern(
        cast(CDValue, value_map[NC_CHEST_XRAY_FINDING_FLAG_1]),
        cast(STValue, value_map[NC_CHEST_XRAY_FINDING_1]),
        NC_CHEST_XRAY_FINDING_FLAG_1,
    )
    if pattern1.status in {STATUS_ALTERNATIVE, STATUS_INVALID}:
        return pattern1

    pattern2 = _chest_xray_finding_pattern(
        cast(CDValue, value_map[NC_CHEST_XRAY_FINDING_FLAG_2]),
        cast(STValue, value_map[NC_CHEST_XRAY_FINDING_2]),
        NC_CHEST_XRAY_FINDING_FLAG_2,
    )
    if pattern2.status in {STATUS_ALTERNATIVE, STATUS_INVALID}:
        return pattern2

    return CheckResult(status=STATUS_MISSING, reason=REASON_MISSING)


def check_4405001001_systolic_blood_pressure(value_map: ValueMap) -> CheckResult:
    """Check that systolic blood pressure has a valid value."""

    return _any_valid(
        value_map,
        (
            NC_SYSTOLIC_BLOOD_PRESSURE_REPRESENTATIVE,
            NC_SYSTOLIC_BLOOD_PRESSURE_SECOND,
            NC_SYSTOLIC_BLOOD_PRESSURE_FIRST,
        ),
    )


def check_4405001002_diastolic_blood_pressure(value_map: ValueMap) -> CheckResult:
    """Check that diastolic blood pressure has a valid value."""

    return _any_valid(
        value_map,
        (
            NC_DIASTOLIC_BLOOD_PRESSURE_REPRESENTATIVE,
            NC_DIASTOLIC_BLOOD_PRESSURE_SECOND,
            NC_DIASTOLIC_BLOOD_PRESSURE_FIRST,
        ),
    )


def check_4406001001_hemoglobin(value_map: ValueMap) -> CheckResult:
    """Check that hemoglobin has a valid value."""

    return _any_valid(value_map, (NC_HEMOGLOBIN,))


def check_4406001002_red_blood_cell_count(value_map: ValueMap) -> CheckResult:
    """Check that red blood cell count has a valid value."""

    return _any_valid(value_map, (NC_RED_BLOOD_CELL_COUNT,))


def check_4407001001_ast(value_map: ValueMap) -> CheckResult:
    """Check that AST has a valid value."""

    return _any_valid(value_map, (NC_AST_JSCC, NC_AST_OTHER))


def check_4407001002_alt(value_map: ValueMap) -> CheckResult:
    """Check that ALT has a valid value."""

    return _any_valid(value_map, (NC_ALT_JSCC, NC_ALT_OTHER))


def check_4407001003_gamma_gt(value_map: ValueMap) -> CheckResult:
    """Check that gamma-GT has a valid value."""

    return _any_valid(value_map, (NC_GAMMA_GT_JSCC, NC_GAMMA_GT_OTHER))


def check_4408001001_ldl(value_map: ValueMap) -> CheckResult:
    """Check that LDL has a valid non-excluded value."""

    return _any_valid(value_map, (NC_LDL_DIRECT, NC_LDL_OTHER_1, NC_LDL_OTHER_2))


def check_4408001002_hdl(value_map: ValueMap) -> CheckResult:
    """Check that HDL has a valid value."""

    return _any_valid(value_map, (NC_HDL_JSCC, NC_HDL_OTHER_1, NC_HDL_OTHER_2))


def check_4408001003_triglycerides(value_map: ValueMap) -> CheckResult:
    """Check that triglycerides have a valid value."""

    return _any_valid(
        value_map,
        (
            NC_TRIGLYCERIDES_FASTING_JSCC,
            NC_TRIGLYCERIDES_FASTING_OTHER_1,
            NC_TRIGLYCERIDES_FASTING_OTHER_2,
            NC_TRIGLYCERIDES_CASUAL_JSCC,
            NC_TRIGLYCERIDES_CASUAL_OTHER_1,
            NC_TRIGLYCERIDES_CASUAL_OTHER_2,
        ),
    )


def check_4409001001_blood_glucose(value_map: ValueMap) -> CheckResult:
    """Check fasting glucose, HbA1c, or conditioned casual glucose."""

    fasting = (
        value_map[NC_FASTING_GLUCOSE_1],
        value_map[NC_FASTING_GLUCOSE_2],
        value_map[NC_FASTING_GLUCOSE_3],
        value_map[NC_FASTING_GLUCOSE_4],
    )
    hba1c = (
        value_map[NC_HBA1C_1],
        value_map[NC_HBA1C_2],
        value_map[NC_HBA1C_3],
        value_map[NC_HBA1C_4],
    )
    casual = (
        value_map[NC_CASUAL_GLUCOSE_1],
        value_map[NC_CASUAL_GLUCOSE_2],
        value_map[NC_CASUAL_GLUCOSE_3],
        value_map[NC_CASUAL_GLUCOSE_4],
    )
    sampling_time = cast(CDValue, value_map[NC_BLOOD_SAMPLING_TIME])
    duplicate = _has_duplicate((*fasting, *hba1c, *casual, sampling_time))
    if duplicate is not None:
        return CheckResult(status=STATUS_INVALID, reason=_duplicate_reason(duplicate))
    if has_any_valid(fasting) or has_any_valid(hba1c):
        return CheckResult(status=STATUS_OK)
    if has_any_valid(casual) and _blood_sampling_time_allows_casual_glucose(sampling_time):
        return CheckResult(status=STATUS_OK)
    return _missing_or_invalid((*fasting, *hba1c, *casual, sampling_time))


def check_4410001001_urine_glucose(value_map: ValueMap) -> CheckResult:
    """Check that urine glucose has a valid value."""

    return _any_valid(value_map, (NC_URINE_GLUCOSE_1, NC_URINE_GLUCOSE_2))


def check_4410001002_urine_protein(value_map: ValueMap) -> CheckResult:
    """Check that urine protein has a valid value."""

    return _any_valid(value_map, (NC_URINE_PROTEIN_1, NC_URINE_PROTEIN_2))


def check_4411001001_electrocardiogram(value_map: ValueMap) -> CheckResult:
    """Check electrocardiogram finding flag and text."""

    return _finding_result(
        value_map,
        NC_ELECTROCARDIOGRAM_FINDING_FLAG,
        NC_ELECTROCARDIOGRAM_FINDING,
    )


ARTICLE44_CHECKERS: dict[str, Checker] = {
    "4401001001": check_4401001001_medical_history,
    "4402001001": check_4402001001_subjective_symptoms,
    "4402001002": check_4402001002_objective_symptoms,
    "4403001001": check_4403001001_height,
    "4403002001": check_4403002001_weight,
    "4403003001": check_4403003001_waist,
    "4403004001": check_4403004001_vision,
    "4403005001": check_4403005001_hearing,
    "4404001001": check_4404001001_chest_xray,
    "4405001001": check_4405001001_systolic_blood_pressure,
    "4405001002": check_4405001002_diastolic_blood_pressure,
    "4406001001": check_4406001001_hemoglobin,
    "4406001002": check_4406001002_red_blood_cell_count,
    "4407001001": check_4407001001_ast,
    "4407001002": check_4407001002_alt,
    "4407001003": check_4407001003_gamma_gt,
    "4408001001": check_4408001001_ldl,
    "4408001002": check_4408001002_hdl,
    "4408001003": check_4408001003_triglycerides,
    "4409001001": check_4409001001_blood_glucose,
    "4410001001": check_4410001001_urine_glucose,
    "4410001002": check_4410001002_urine_protein,
    "4411001001": check_4411001001_electrocardiogram,
}


def check_article44(
    value_map: ValueMap,
) -> Article44Result:
    """Run all Article 44 checkers in detail-number order."""

    return {
        detail_no: checker(value_map)
        for detail_no, checker in ARTICLE44_CHECKERS.items()
    }
