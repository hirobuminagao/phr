from decimal import Decimal
import re

import pytest

from scripts.from_medical.script_lib import article44_checker as checker
from scripts.from_medical.script_lib.article44_models import (
    CDValue,
    CheckResult,
    PQValue,
    STValue,
    ValueInvalidReason,
    ValueMap,
    ValueState,
)
from scripts.lib.examination.models import (
    REASON_MISSING,
    STATUS_ALTERNATIVE,
    STATUS_INVALID,
    STATUS_MISSING,
    STATUS_OK,
)


VALID_STATUSES = {STATUS_OK, "CALCULATED", STATUS_ALTERNATIVE, STATUS_MISSING, STATUS_INVALID}
EXPECTED_DETAIL_NOS = (
    "4401001001",
    "4402001001",
    "4402001002",
    "4403001001",
    "4403002001",
    "4403003001",
    "4403004001",
    "4403005001",
    "4404001001",
    "4405001001",
    "4405001002",
    "4406001001",
    "4406001002",
    "4407001001",
    "4407001002",
    "4407001003",
    "4408001001",
    "4408001002",
    "4408001003",
    "4409001001",
    "4410001001",
    "4410001002",
    "4411001001",
)


def pq(
    value: str | Decimal | None = "1",
    *,
    state: ValueState = ValueState.PRESENT,
    valid: bool = True,
    reason: ValueInvalidReason | None = None,
    duplicate_count: int | None = None,
    unit: str | None = None,
) -> PQValue:
    numeric_value = Decimal(str(value)) if value is not None and valid else None
    return PQValue(
        value_state=state,
        raw_value=str(value) if value is not None else None,
        numeric_value=numeric_value,
        unit=unit,
        is_valid=valid,
        invalid_reason=reason,
        duplicate_count=duplicate_count,
    )


def cd(
    code: str | None = "1",
    *,
    state: ValueState = ValueState.PRESENT,
    valid: bool = True,
    reason: ValueInvalidReason | None = None,
    duplicate_count: int | None = None,
) -> CDValue:
    return CDValue(
        value_state=state,
        raw_value=code,
        code_value=code,
        is_valid=valid,
        invalid_reason=reason,
        duplicate_count=duplicate_count,
    )


def st(
    text: str | None = "所見あり",
    *,
    state: ValueState = ValueState.PRESENT,
    valid: bool = True,
    reason: ValueInvalidReason | None = None,
    duplicate_count: int | None = None,
) -> STValue:
    return STValue(
        value_state=state,
        raw_text=text,
        text=text,
        is_valid=valid,
        invalid_reason=reason,
        duplicate_count=duplicate_count,
    )


def not_found_pq() -> PQValue:
    return pq(None, state=ValueState.NOT_FOUND, valid=False)


def not_found_cd() -> CDValue:
    return cd(None, state=ValueState.NOT_FOUND, valid=False)


def not_found_st() -> STValue:
    return st(None, state=ValueState.NOT_FOUND, valid=False)


def duplicate_pq(count: int = 2) -> PQValue:
    return pq(None, valid=False, reason=ValueInvalidReason.DUPLICATE_NAMECODE, duplicate_count=count)


def duplicate_cd(count: int = 2) -> CDValue:
    return cd(None, valid=False, reason=ValueInvalidReason.DUPLICATE_NAMECODE, duplicate_count=count)


def duplicate_st(count: int = 2) -> STValue:
    return st(None, valid=False, reason=ValueInvalidReason.DUPLICATE_NAMECODE, duplicate_count=count)


def type_mismatch_pq() -> PQValue:
    return pq(None, valid=False, reason=ValueInvalidReason.TYPE_MISMATCH)


def type_mismatch_cd() -> CDValue:
    return cd(None, valid=False, reason=ValueInvalidReason.TYPE_MISMATCH)


def base_value_map() -> ValueMap:
    return {
        checker.NC_MEDICAL_HISTORY_FINDING_FLAG: cd("2"),
        checker.NC_MEDICAL_HISTORY_FINDING: not_found_st(),
        checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG: cd("2"),
        checker.NC_SUBJECTIVE_SYMPTOMS_FINDING: not_found_st(),
        checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG: cd("2"),
        checker.NC_OBJECTIVE_SYMPTOMS_FINDING: not_found_st(),
        checker.NC_HEIGHT: pq("170"),
        checker.NC_WEIGHT: pq("65"),
        checker.NC_WAIST_MEASURED: pq("80"),
        checker.NC_WAIST_SELF_MEASURED: not_found_pq(),
        checker.NC_WAIST_SELF_REPORTED: not_found_pq(),
        checker.NC_BMI: pq("21"),
        checker.NC_RIGHT_VISION_UNCORRECTED: pq("1.0"),
        checker.NC_LEFT_VISION_UNCORRECTED: pq("1.0"),
        checker.NC_RIGHT_VISION_CORRECTED: not_found_pq(),
        checker.NC_LEFT_VISION_CORRECTED: not_found_pq(),
        checker.NC_HEARING_RIGHT_1000HZ: cd("1"),
        checker.NC_HEARING_RIGHT_4000HZ: cd("1"),
        checker.NC_HEARING_LEFT_1000HZ: cd("1"),
        checker.NC_HEARING_LEFT_4000HZ: cd("1"),
        checker.NC_HEARING_CONVERSATION: not_found_st(),
        checker.NC_CHEST_XRAY_RESULT_1: cd("1"),
        checker.NC_CHEST_XRAY_RESULT_2: not_found_cd(),
        checker.NC_CHEST_XRAY_FINDING_FLAG_1: not_found_cd(),
        checker.NC_CHEST_XRAY_FINDING_1: not_found_st(),
        checker.NC_CHEST_XRAY_FINDING_FLAG_2: not_found_cd(),
        checker.NC_CHEST_XRAY_FINDING_2: not_found_st(),
        checker.NC_SYSTOLIC_BLOOD_PRESSURE_REPRESENTATIVE: pq("120"),
        checker.NC_SYSTOLIC_BLOOD_PRESSURE_SECOND: not_found_pq(),
        checker.NC_SYSTOLIC_BLOOD_PRESSURE_FIRST: not_found_pq(),
        checker.NC_DIASTOLIC_BLOOD_PRESSURE_REPRESENTATIVE: pq("80"),
        checker.NC_DIASTOLIC_BLOOD_PRESSURE_SECOND: not_found_pq(),
        checker.NC_DIASTOLIC_BLOOD_PRESSURE_FIRST: not_found_pq(),
        checker.NC_HEMOGLOBIN: pq("14"),
        checker.NC_RED_BLOOD_CELL_COUNT: pq("450"),
        checker.NC_AST_JSCC: pq("20"),
        checker.NC_AST_OTHER: not_found_pq(),
        checker.NC_ALT_JSCC: pq("20"),
        checker.NC_ALT_OTHER: not_found_pq(),
        checker.NC_GAMMA_GT_JSCC: pq("30"),
        checker.NC_GAMMA_GT_OTHER: not_found_pq(),
        checker.NC_LDL_DIRECT: pq("100"),
        checker.NC_LDL_OTHER_1: not_found_pq(),
        checker.NC_LDL_OTHER_2: not_found_pq(),
        checker.NC_HDL_JSCC: pq("60"),
        checker.NC_HDL_OTHER_1: not_found_pq(),
        checker.NC_HDL_OTHER_2: not_found_pq(),
        checker.NC_TRIGLYCERIDES_FASTING_JSCC: pq("100"),
        checker.NC_TRIGLYCERIDES_FASTING_OTHER_1: not_found_pq(),
        checker.NC_TRIGLYCERIDES_FASTING_OTHER_2: not_found_pq(),
        checker.NC_TRIGLYCERIDES_CASUAL_JSCC: not_found_pq(),
        checker.NC_TRIGLYCERIDES_CASUAL_OTHER_1: not_found_pq(),
        checker.NC_TRIGLYCERIDES_CASUAL_OTHER_2: not_found_pq(),
        checker.NC_FASTING_GLUCOSE_1: pq("90"),
        checker.NC_FASTING_GLUCOSE_2: not_found_pq(),
        checker.NC_FASTING_GLUCOSE_3: not_found_pq(),
        checker.NC_FASTING_GLUCOSE_4: not_found_pq(),
        checker.NC_HBA1C_1: not_found_pq(),
        checker.NC_HBA1C_2: not_found_pq(),
        checker.NC_HBA1C_3: not_found_pq(),
        checker.NC_HBA1C_4: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_1: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_2: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_3: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_4: not_found_pq(),
        checker.NC_BLOOD_SAMPLING_TIME: cd("2"),
        checker.NC_URINE_GLUCOSE_1: cd("1"),
        checker.NC_URINE_GLUCOSE_2: not_found_cd(),
        checker.NC_URINE_PROTEIN_1: cd("1"),
        checker.NC_URINE_PROTEIN_2: not_found_cd(),
        checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG: cd("2"),
        checker.NC_ELECTROCARDIOGRAM_FINDING: not_found_st(),
    }


def assert_status(result: CheckResult, expected_status: str, expected_reason: str | None = None) -> None:
    assert result.status == expected_status
    assert result.reason == expected_reason
    assert result.status in VALID_STATUSES
    assert result.status not in {"WARNING", "NG"}


@pytest.mark.parametrize(("detail_no", "checker_func"), tuple(checker.ARTICLE44_CHECKERS.items()))
def test_article44_checker_smoke_returns_normal_status(detail_no: str, checker_func: checker.Checker) -> None:
    result = checker_func(base_value_map())

    assert detail_no in EXPECTED_DETAIL_NOS
    assert isinstance(result, CheckResult)
    assert result.status in {STATUS_OK, STATUS_ALTERNATIVE, "CALCULATED"}
    assert result.status not in {STATUS_MISSING, STATUS_INVALID, "WARNING", "NG"}


@pytest.mark.parametrize(
    ("checker_func", "flag_namecode", "text_namecode"),
    [
        (
            checker.check_4401001001_medical_history,
            checker.NC_MEDICAL_HISTORY_FINDING_FLAG,
            checker.NC_MEDICAL_HISTORY_FINDING,
        ),
        (
            checker.check_4402001001_subjective_symptoms,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4402001002_objective_symptoms,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4411001001_electrocardiogram,
            checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG,
            checker.NC_ELECTROCARDIOGRAM_FINDING,
        ),
    ],
)
def test_finding_checkers_accept_absent_finding_without_text(
    checker_func: checker.Checker,
    flag_namecode: str,
    text_namecode: str,
) -> None:
    result = checker_func({flag_namecode: cd("2"), text_namecode: not_found_st()})

    assert_status(result, STATUS_OK)


@pytest.mark.parametrize(
    ("checker_func", "flag_namecode", "text_namecode"),
    [
        (
            checker.check_4401001001_medical_history,
            checker.NC_MEDICAL_HISTORY_FINDING_FLAG,
            checker.NC_MEDICAL_HISTORY_FINDING,
        ),
        (
            checker.check_4402001001_subjective_symptoms,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4402001002_objective_symptoms,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4411001001_electrocardiogram,
            checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG,
            checker.NC_ELECTROCARDIOGRAM_FINDING,
        ),
    ],
)
def test_finding_checkers_accept_present_finding_with_text(
    checker_func: checker.Checker,
    flag_namecode: str,
    text_namecode: str,
) -> None:
    result = checker_func({flag_namecode: cd("1"), text_namecode: st("詳細あり")})

    assert_status(result, STATUS_OK)


@pytest.mark.parametrize(
    ("checker_func", "flag_namecode", "text_namecode"),
    [
        (
            checker.check_4401001001_medical_history,
            checker.NC_MEDICAL_HISTORY_FINDING_FLAG,
            checker.NC_MEDICAL_HISTORY_FINDING,
        ),
        (
            checker.check_4402001001_subjective_symptoms,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4402001002_objective_symptoms,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4411001001_electrocardiogram,
            checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG,
            checker.NC_ELECTROCARDIOGRAM_FINDING,
        ),
    ],
)
def test_finding_checkers_return_missing_when_present_finding_has_no_text(
    checker_func: checker.Checker,
    flag_namecode: str,
    text_namecode: str,
) -> None:
    result = checker_func({flag_namecode: cd("1"), text_namecode: not_found_st()})

    assert_status(result, STATUS_MISSING, REASON_MISSING)


@pytest.mark.parametrize(
    ("checker_func", "flag_namecode", "text_namecode"),
    [
        (
            checker.check_4401001001_medical_history,
            checker.NC_MEDICAL_HISTORY_FINDING_FLAG,
            checker.NC_MEDICAL_HISTORY_FINDING,
        ),
        (
            checker.check_4402001001_subjective_symptoms,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4402001002_objective_symptoms,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4411001001_electrocardiogram,
            checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG,
            checker.NC_ELECTROCARDIOGRAM_FINDING,
        ),
    ],
)
def test_finding_checkers_return_invalid_for_invalid_cd(
    checker_func: checker.Checker,
    flag_namecode: str,
    text_namecode: str,
) -> None:
    result = checker_func({flag_namecode: type_mismatch_cd(), text_namecode: st("詳細")})

    assert_status(result, STATUS_INVALID, ValueInvalidReason.TYPE_MISMATCH.value)


@pytest.mark.parametrize(
    ("checker_func", "flag_namecode", "text_namecode"),
    [
        (
            checker.check_4401001001_medical_history,
            checker.NC_MEDICAL_HISTORY_FINDING_FLAG,
            checker.NC_MEDICAL_HISTORY_FINDING,
        ),
        (
            checker.check_4402001001_subjective_symptoms,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_SUBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4402001002_objective_symptoms,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING_FLAG,
            checker.NC_OBJECTIVE_SYMPTOMS_FINDING,
        ),
        (
            checker.check_4411001001_electrocardiogram,
            checker.NC_ELECTROCARDIOGRAM_FINDING_FLAG,
            checker.NC_ELECTROCARDIOGRAM_FINDING,
        ),
    ],
)
def test_finding_checkers_return_invalid_for_duplicate_st(
    checker_func: checker.Checker,
    flag_namecode: str,
    text_namecode: str,
) -> None:
    result = checker_func({flag_namecode: cd("1"), text_namecode: duplicate_st()})

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def waist_map(
    measured: PQValue,
    self_measured: PQValue,
    self_reported: PQValue,
    bmi: PQValue,
) -> ValueMap:
    return {
        checker.NC_WAIST_MEASURED: measured,
        checker.NC_WAIST_SELF_MEASURED: self_measured,
        checker.NC_WAIST_SELF_REPORTED: self_reported,
        checker.NC_BMI: bmi,
    }


def test_waist_accepts_measured_value_and_prioritizes_it() -> None:
    result = checker.check_4403003001_waist(
        waist_map(pq("80"), pq("82"), not_found_pq(), pq("25"))
    )

    assert_status(result, STATUS_OK)


def test_waist_accepts_self_measured_when_measured_missing() -> None:
    result = checker.check_4403003001_waist(
        waist_map(not_found_pq(), pq("82"), not_found_pq(), pq("25"))
    )

    assert_status(result, STATUS_OK)


@pytest.mark.parametrize(
    ("bmi", "expected_status"),
    [
        ("21.9", STATUS_OK),
        ("22", STATUS_MISSING),
        ("22.1", STATUS_MISSING),
    ],
)
def test_waist_self_reported_depends_on_bmi_boundary(bmi: str, expected_status: str) -> None:
    result = checker.check_4403003001_waist(
        waist_map(not_found_pq(), not_found_pq(), pq("84"), pq(bmi))
    )

    assert result.status == expected_status


def test_waist_returns_missing_when_all_candidates_missing() -> None:
    result = checker.check_4403003001_waist(
        waist_map(not_found_pq(), not_found_pq(), not_found_pq(), not_found_pq())
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_waist_returns_invalid_for_duplicate_standard_value() -> None:
    result = checker.check_4403003001_waist(
        waist_map(duplicate_pq(), not_found_pq(), not_found_pq(), pq("21"))
    )

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def vision_map(
    right_uncorrected: PQValue,
    left_uncorrected: PQValue,
    right_corrected: PQValue,
    left_corrected: PQValue,
) -> ValueMap:
    return {
        checker.NC_RIGHT_VISION_UNCORRECTED: right_uncorrected,
        checker.NC_LEFT_VISION_UNCORRECTED: left_uncorrected,
        checker.NC_RIGHT_VISION_CORRECTED: right_corrected,
        checker.NC_LEFT_VISION_CORRECTED: left_corrected,
    }


def test_vision_accepts_uncorrected_both_sides_and_prioritizes_any_side_candidates() -> None:
    result = checker.check_4403004001_vision(vision_map(pq("1.0"), pq("1.0"), pq("1.2"), pq("1.2")))

    assert_status(result, STATUS_OK)


def test_vision_accepts_corrected_when_uncorrected_missing() -> None:
    result = checker.check_4403004001_vision(
        vision_map(not_found_pq(), not_found_pq(), pq("1.2"), pq("1.2"))
    )

    assert_status(result, STATUS_OK)


def test_vision_returns_missing_when_one_side_missing() -> None:
    result = checker.check_4403004001_vision(
        vision_map(pq("1.0"), not_found_pq(), not_found_pq(), not_found_pq())
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_vision_returns_invalid_for_invalid_candidate() -> None:
    result = checker.check_4403004001_vision(
        vision_map(type_mismatch_pq(), pq("1.0"), not_found_pq(), not_found_pq())
    )

    assert_status(result, STATUS_INVALID, ValueInvalidReason.TYPE_MISMATCH.value)


def hearing_map(
    right_1000hz: CDValue,
    right_4000hz: CDValue,
    left_1000hz: CDValue,
    left_4000hz: CDValue,
    conversation: STValue,
) -> ValueMap:
    return {
        checker.NC_HEARING_RIGHT_1000HZ: right_1000hz,
        checker.NC_HEARING_RIGHT_4000HZ: right_4000hz,
        checker.NC_HEARING_LEFT_1000HZ: left_1000hz,
        checker.NC_HEARING_LEFT_4000HZ: left_4000hz,
        checker.NC_HEARING_CONVERSATION: conversation,
    }


def test_hearing_accepts_standard_values_and_prioritizes_them() -> None:
    result = checker.check_4403005001_hearing(hearing_map(cd("1"), cd("1"), cd("1"), cd("1"), st("会話法")))

    assert_status(result, STATUS_OK)


def test_hearing_uses_conversation_as_alternative_when_standard_incomplete() -> None:
    result = checker.check_4403005001_hearing(
        hearing_map(cd("1"), cd("1"), cd("1"), not_found_cd(), st("会話法"))
    )

    assert_status(result, STATUS_ALTERNATIVE, f"ALTERNATIVE:{checker.NC_HEARING_CONVERSATION}")


def test_hearing_returns_missing_without_standard_or_conversation() -> None:
    result = checker.check_4403005001_hearing(
        hearing_map(cd("1"), cd("1"), cd("1"), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_hearing_returns_invalid_for_duplicate_standard_value() -> None:
    result = checker.check_4403005001_hearing(
        hearing_map(duplicate_cd(), cd("1"), cd("1"), cd("1"), st("会話法"))
    )

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def test_hearing_returns_invalid_for_duplicate_conversation_when_fallback_needed() -> None:
    result = checker.check_4403005001_hearing(
        hearing_map(cd("1"), cd("1"), cd("1"), not_found_cd(), duplicate_st())
    )

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def test_hearing_returns_invalid_for_standard_type_mismatch() -> None:
    result = checker.check_4403005001_hearing(
        hearing_map(type_mismatch_cd(), cd("1"), cd("1"), cd("1"), st("会話法"))
    )

    assert_status(result, STATUS_INVALID, ValueInvalidReason.TYPE_MISMATCH.value)


def chest_xray_map(
    result1: CDValue,
    result2: CDValue,
    flag1: CDValue,
    text1: STValue,
    flag2: CDValue,
    text2: STValue,
) -> ValueMap:
    return {
        checker.NC_CHEST_XRAY_RESULT_1: result1,
        checker.NC_CHEST_XRAY_RESULT_2: result2,
        checker.NC_CHEST_XRAY_FINDING_FLAG_1: flag1,
        checker.NC_CHEST_XRAY_FINDING_1: text1,
        checker.NC_CHEST_XRAY_FINDING_FLAG_2: flag2,
        checker.NC_CHEST_XRAY_FINDING_2: text2,
    }


def test_chest_xray_accepts_standard_result_and_prioritizes_it() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(cd("1"), not_found_cd(), cd("1"), st("所見あり"), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_OK)


def test_chest_xray_uses_finding_pattern1_as_alternative() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(not_found_cd(), not_found_cd(), cd("1"), st("所見あり"), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_ALTERNATIVE, f"ALTERNATIVE:{checker.NC_CHEST_XRAY_FINDING_FLAG_1}")


def test_chest_xray_uses_finding_pattern2_as_alternative() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(not_found_cd(), not_found_cd(), cd("2"), not_found_st(), cd("1"), st("所見あり"))
    )

    assert_status(result, STATUS_ALTERNATIVE, f"ALTERNATIVE:{checker.NC_CHEST_XRAY_FINDING_FLAG_1}")


def test_chest_xray_returns_missing_when_all_candidates_missing() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(
            not_found_cd(),
            not_found_cd(),
            not_found_cd(),
            not_found_st(),
            not_found_cd(),
            not_found_st(),
        )
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_chest_xray_returns_invalid_when_present_finding_has_no_text() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(not_found_cd(), not_found_cd(), cd("1"), not_found_st(), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_INVALID, "FINDING_TEXT_MISSING")


def test_chest_xray_returns_invalid_for_duplicate_result() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(duplicate_cd(), not_found_cd(), not_found_cd(), not_found_st(), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def test_chest_xray_returns_invalid_for_standard_type_mismatch() -> None:
    result = checker.check_4404001001_chest_xray(
        chest_xray_map(type_mismatch_cd(), not_found_cd(), not_found_cd(), not_found_st(), not_found_cd(), not_found_st())
    )

    assert_status(result, STATUS_INVALID, ValueInvalidReason.TYPE_MISMATCH.value)


def blood_glucose_map(
    fasting: PQValue,
    hba1c: PQValue,
    casual: PQValue,
    sampling_time: CDValue,
) -> ValueMap:
    return {
        checker.NC_FASTING_GLUCOSE_1: fasting,
        checker.NC_FASTING_GLUCOSE_2: not_found_pq(),
        checker.NC_FASTING_GLUCOSE_3: not_found_pq(),
        checker.NC_FASTING_GLUCOSE_4: not_found_pq(),
        checker.NC_HBA1C_1: hba1c,
        checker.NC_HBA1C_2: not_found_pq(),
        checker.NC_HBA1C_3: not_found_pq(),
        checker.NC_HBA1C_4: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_1: casual,
        checker.NC_CASUAL_GLUCOSE_2: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_3: not_found_pq(),
        checker.NC_CASUAL_GLUCOSE_4: not_found_pq(),
        checker.NC_BLOOD_SAMPLING_TIME: sampling_time,
    }


def test_blood_glucose_accepts_fasting_and_prioritizes_it() -> None:
    result = checker.check_4409001001_blood_glucose(blood_glucose_map(pq("90"), pq("5.5"), pq("100"), cd("1")))

    assert_status(result, STATUS_OK)


def test_blood_glucose_accepts_hba1c_when_fasting_missing() -> None:
    result = checker.check_4409001001_blood_glucose(blood_glucose_map(not_found_pq(), pq("5.5"), not_found_pq(), cd("1")))

    assert_status(result, STATUS_OK)


@pytest.mark.parametrize("sampling_code", ["2", "3"])
def test_blood_glucose_accepts_casual_when_sampling_condition_allows(sampling_code: str) -> None:
    result = checker.check_4409001001_blood_glucose(
        blood_glucose_map(not_found_pq(), not_found_pq(), pq("100"), cd(sampling_code))
    )

    assert_status(result, STATUS_OK)


def test_blood_glucose_rejects_casual_when_sampling_condition_does_not_allow() -> None:
    result = checker.check_4409001001_blood_glucose(
        blood_glucose_map(not_found_pq(), not_found_pq(), pq("100"), cd("1"))
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_blood_glucose_returns_missing_when_all_candidates_missing() -> None:
    result = checker.check_4409001001_blood_glucose(
        blood_glucose_map(not_found_pq(), not_found_pq(), not_found_pq(), not_found_cd())
    )

    assert_status(result, STATUS_MISSING, REASON_MISSING)


def test_blood_glucose_returns_invalid_for_duplicate_reference_value() -> None:
    result = checker.check_4409001001_blood_glucose(
        blood_glucose_map(duplicate_pq(), not_found_pq(), not_found_pq(), cd("2"))
    )

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


def test_blood_glucose_returns_invalid_for_invalid_sampling_time_cd() -> None:
    result = checker.check_4409001001_blood_glucose(
        blood_glucose_map(not_found_pq(), not_found_pq(), pq("100"), type_mismatch_cd())
    )

    assert_status(result, STATUS_INVALID, ValueInvalidReason.TYPE_MISMATCH.value)


SIMPLE_CHECKERS: tuple[tuple[checker.Checker, tuple[str, ...], str], ...] = (
    (checker.check_4403001001_height, (checker.NC_HEIGHT,), "PQ"),
    (checker.check_4403002001_weight, (checker.NC_WEIGHT,), "PQ"),
    (
        checker.check_4405001001_systolic_blood_pressure,
        (
            checker.NC_SYSTOLIC_BLOOD_PRESSURE_REPRESENTATIVE,
            checker.NC_SYSTOLIC_BLOOD_PRESSURE_SECOND,
            checker.NC_SYSTOLIC_BLOOD_PRESSURE_FIRST,
        ),
        "PQ",
    ),
    (
        checker.check_4405001002_diastolic_blood_pressure,
        (
            checker.NC_DIASTOLIC_BLOOD_PRESSURE_REPRESENTATIVE,
            checker.NC_DIASTOLIC_BLOOD_PRESSURE_SECOND,
            checker.NC_DIASTOLIC_BLOOD_PRESSURE_FIRST,
        ),
        "PQ",
    ),
    (checker.check_4406001001_hemoglobin, (checker.NC_HEMOGLOBIN,), "PQ"),
    (checker.check_4406001002_red_blood_cell_count, (checker.NC_RED_BLOOD_CELL_COUNT,), "PQ"),
    (checker.check_4407001001_ast, (checker.NC_AST_JSCC, checker.NC_AST_OTHER), "PQ"),
    (checker.check_4407001002_alt, (checker.NC_ALT_JSCC, checker.NC_ALT_OTHER), "PQ"),
    (checker.check_4407001003_gamma_gt, (checker.NC_GAMMA_GT_JSCC, checker.NC_GAMMA_GT_OTHER), "PQ"),
    (checker.check_4408001001_ldl, (checker.NC_LDL_DIRECT, checker.NC_LDL_OTHER_1, checker.NC_LDL_OTHER_2), "PQ"),
    (checker.check_4408001002_hdl, (checker.NC_HDL_JSCC, checker.NC_HDL_OTHER_1, checker.NC_HDL_OTHER_2), "PQ"),
    (
        checker.check_4408001003_triglycerides,
        (
            checker.NC_TRIGLYCERIDES_FASTING_JSCC,
            checker.NC_TRIGLYCERIDES_FASTING_OTHER_1,
            checker.NC_TRIGLYCERIDES_FASTING_OTHER_2,
            checker.NC_TRIGLYCERIDES_CASUAL_JSCC,
            checker.NC_TRIGLYCERIDES_CASUAL_OTHER_1,
            checker.NC_TRIGLYCERIDES_CASUAL_OTHER_2,
        ),
        "PQ",
    ),
    (checker.check_4410001001_urine_glucose, (checker.NC_URINE_GLUCOSE_1, checker.NC_URINE_GLUCOSE_2), "CD"),
    (checker.check_4410001002_urine_protein, (checker.NC_URINE_PROTEIN_1, checker.NC_URINE_PROTEIN_2), "CD"),
)


def missing_value(value_type: str) -> PQValue | CDValue:
    return not_found_cd() if value_type == "CD" else not_found_pq()


def valid_value(value_type: str) -> PQValue | CDValue:
    return cd("1") if value_type == "CD" else pq("1")


def duplicate_value(value_type: str) -> PQValue | CDValue:
    return duplicate_cd() if value_type == "CD" else duplicate_pq()


@pytest.mark.parametrize(("checker_func", "namecodes", "value_type"), SIMPLE_CHECKERS)
def test_simple_checkers_accept_any_valid_value(
    checker_func: checker.Checker,
    namecodes: tuple[str, ...],
    value_type: str,
) -> None:
    value_map = {namecode: missing_value(value_type) for namecode in namecodes}
    value_map[namecodes[-1]] = valid_value(value_type)

    result = checker_func(value_map)

    assert_status(result, STATUS_OK)


@pytest.mark.parametrize(("checker_func", "namecodes", "value_type"), SIMPLE_CHECKERS)
def test_simple_checkers_return_missing_when_all_candidates_not_found(
    checker_func: checker.Checker,
    namecodes: tuple[str, ...],
    value_type: str,
) -> None:
    result = checker_func({namecode: missing_value(value_type) for namecode in namecodes})

    assert_status(result, STATUS_MISSING, REASON_MISSING)


@pytest.mark.parametrize(("checker_func", "namecodes", "value_type"), SIMPLE_CHECKERS)
def test_simple_checkers_return_invalid_for_duplicate_candidate(
    checker_func: checker.Checker,
    namecodes: tuple[str, ...],
    value_type: str,
) -> None:
    value_map = {namecode: missing_value(value_type) for namecode in namecodes}
    value_map[namecodes[0]] = duplicate_value(value_type)

    result = checker_func(value_map)

    assert_status(result, STATUS_INVALID, "DUPLICATE_NAMECODE:count=2")


@pytest.mark.parametrize(
    ("mutated_namecode", "duplicate_value"),
    [
        (checker.NC_HEIGHT, duplicate_pq()),
        (checker.NC_URINE_GLUCOSE_1, duplicate_cd()),
        (checker.NC_MEDICAL_HISTORY_FINDING, duplicate_st()),
    ],
)
def test_check_article44_marks_only_affected_duplicate_item_invalid(
    mutated_namecode: str,
    duplicate_value: PQValue | CDValue | STValue,
) -> None:
    value_map = base_value_map()
    value_map[mutated_namecode] = duplicate_value

    result = checker.check_article44(value_map)

    invalid_results = {
        detail_no: check_result
        for detail_no, check_result in result.items()
        if check_result.status == STATUS_INVALID
    }
    assert invalid_results
    assert all(check_result.reason == "DUPLICATE_NAMECODE:count=2" for check_result in invalid_results.values())
    assert len(result) == 23


def test_check_article44_returns_23_results_in_checker_order() -> None:
    result = checker.check_article44(base_value_map())

    assert tuple(result) == EXPECTED_DETAIL_NOS
    assert tuple(result) == tuple(checker.ARTICLE44_CHECKERS)
    assert len(result) == 23
    assert set(EXPECTED_DETAIL_NOS).issubset(result)
    assert "4401001002" not in result
    assert "4404002001" not in result
    assert all(isinstance(check_result, CheckResult) for check_result in result.values())
    assert all(check_result.status in VALID_STATUSES for check_result in result.values())
    assert all(check_result.status not in {"WARNING", "NG"} for check_result in result.values())


def test_check_article44_is_stable_for_same_value_map() -> None:
    value_map = base_value_map()

    first = checker.check_article44(value_map)
    second = checker.check_article44(value_map)

    assert first == second
    assert tuple(first) == tuple(second)


def test_article44_checkers_registration_contract() -> None:
    assert len(checker.ARTICLE44_CHECKERS) == 23
    assert tuple(checker.ARTICLE44_CHECKERS) == EXPECTED_DETAIL_NOS
    assert len(set(checker.ARTICLE44_CHECKERS)) == 23
    assert all(callable(checker_func) for checker_func in checker.ARTICLE44_CHECKERS.values())
    for detail_no, checker_func in checker.ARTICLE44_CHECKERS.items():
        assert detail_no[:4] in checker_func.__name__
