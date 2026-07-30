"""Health examination report classification helpers."""

from __future__ import annotations

from datetime import date


SPECIFIC_REPORT_CATEGORY = "10"
SPECIFIC_PROGRAM_CODE = "010"
OTHER_REPORT_CATEGORY = "40"
OTHER_PROGRAM_CODE = "990"


def calculate_full_age(birthdate: date, reference_date: date) -> int:
    return reference_date.year - birthdate.year - (
        (reference_date.month, reference_date.day) < (birthdate.month, birthdate.day)
    )


def classify_report_codes_by_age(
    *,
    birthdate: date,
    reference_date: date,
) -> tuple[str, str]:
    age = calculate_full_age(birthdate, reference_date)
    if 40 <= age <= 74:
        return SPECIFIC_REPORT_CATEGORY, SPECIFIC_PROGRAM_CODE
    return OTHER_REPORT_CATEGORY, OTHER_PROGRAM_CODE


def resolve_age_reference_date(
    *,
    age_rule_type: str | None,
    age_reference_date: date | None,
    exam_date: date | None,
) -> date | None:
    rule_type = (age_rule_type or "").strip().upper()
    if rule_type == "EXAM_DATE":
        return exam_date
    if rule_type == "FIXED_DATE":
        return age_reference_date
    return None
