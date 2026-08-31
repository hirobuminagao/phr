from apps.health_exam_admin.main import normalize_basic_info_correction_value


def test_report_code_correction_requires_both_codes() -> None:
    assert normalize_basic_info_correction_value("report_codes", "10|") == (
        None,
        "ERROR",
        "REPORT_CATEGORY_AND_PROGRAM_CODE_REQUIRED",
    )


def test_report_code_correction_accepts_two_and_three_digit_codes() -> None:
    assert normalize_basic_info_correction_value("report_codes", "10|010") == (
        "10|010",
        "OK",
        None,
    )
