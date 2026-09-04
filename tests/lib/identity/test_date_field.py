from __future__ import annotations

from scripts.lib.identity.field.birthdate import normalize_birthdate
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact


def test_normalize_birthdate_accepts_excel_serial_date() -> None:
    result = normalize_birthdate("27599")

    assert result["ok"] is True
    assert result["field_norm"] == "1975-07-24"
    assert result["match"] == "19750724"
    assert result["reason"] == "excel_serial_date"


def test_normalize_birthdate_keeps_standard_date_inputs() -> None:
    result = normalize_birthdate("1975/07/24")

    assert result["ok"] is True
    assert result["field_norm"] == "1975-07-24"
    assert result["match"] == "19750724"
    assert result["reason"] is None


def test_normalize_birthdate_accepts_non_zero_padded_date_parts() -> None:
    slash_result = normalize_birthdate("1977/3/10")
    hyphen_result = normalize_birthdate("1977-3-10")

    assert slash_result["ok"] is True
    assert slash_result["field_norm"] == "1977-03-10"
    assert slash_result["match"] == "19770310"
    assert hyphen_result["ok"] is True
    assert hyphen_result["field_norm"] == "1977-03-10"


def test_normalize_birthdate_rejects_year_month_like_serial_false_positive() -> None:
    result = normalize_birthdate("202605")

    assert result["ok"] is False
    assert result["reason"] == "invalid_date_format"


def test_normalize_exam_date_accepts_excel_serial_date() -> None:
    result = normalize_date_to_ymd_and_compact("46254", purpose="exam_date")

    assert result["ok"] is True
    assert result["field_norm"] == "2026-08-20"
    assert result["match"] == "20260820"
    assert result["reason"] == "excel_serial_date"
