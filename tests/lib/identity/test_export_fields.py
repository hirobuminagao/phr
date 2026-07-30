from __future__ import annotations

from datetime import date

import pytest

from scripts.lib.identity.export_fields import ExportFieldError, build_xml_export_fields
from scripts.lib.identity.field.phone_number import normalize_phone_number_export


def test_build_xml_export_fields_uses_shared_identity_normalizers() -> None:
    fields = build_xml_export_fields(
        {
            "insurer_number": "6139463",
            "insurance_symbol_raw": "ＡＢ-０１",
            "insurance_number_raw": "００１２３",
            "name_kana_raw": "やまだ たろう",
            "gender_code": "男",
            "birthdate": date(1980, 1, 2),
            "exam_date": date(2026, 6, 3),
            "postal_code": "1234567",
            "address": "東京都 千代田区 1-2",
        }
    )

    assert fields.insurer_number == "06139463"
    assert fields.insurance_symbol == "ＡＢ－０１"
    assert fields.insurance_number == "123"
    assert fields.name_kana == "ヤマダ　タロウ"
    assert fields.gender_code == "1"
    assert fields.birthdate == "19800102"
    assert fields.exam_date == "20260603"
    assert fields.postal_code == "123-4567"


def test_build_xml_export_fields_rejects_missing_required_value() -> None:
    with pytest.raises(ExportFieldError, match="insurance_symbol"):
        build_xml_export_fields(
            {
                "insurer_number": "6139463",
                "insurance_symbol_raw": None,
                "insurance_number_raw": "123",
                "name_kana_raw": "ヤマダ タロウ",
                "gender_code": "1",
                "birthdate": "19800102",
                "exam_date": "20260603",
            }
        )


def test_numeric_only_insurance_symbol_is_halfwidth() -> None:
    fields = build_xml_export_fields(
        {
            "insurer_number": "6139463",
            "insurance_symbol_raw": "００１２３",
            "insurance_number_raw": "456",
            "name_kana_raw": "ヤマダ タロウ",
            "gender_code": "1",
            "birthdate": "19800102",
            "exam_date": "20260603",
        }
    )
    assert fields.insurance_symbol == "00123"


def test_phone_number_export_uses_ascii_digits() -> None:
    assert normalize_phone_number_export("ＴＥＬ：０３－１２３４－５６７８") == "tel:0312345678"
