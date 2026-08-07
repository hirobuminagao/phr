"""Common health exam result value normalizer.

Initial scope:
- Preserve raw values.
- Normalize common text shape with the existing identity base normalizer.
- Classify non-result / unmeasurable words before type conversion.
- Resolve CD/CO variants through phr_master.norm_variants.
- Parse numeric values without unit conversion.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping

from scripts.lib.db.lookup.exam_item_master import get_exam_item
from scripts.lib.db.lookup.norm_variant import get_norm_variant
from scripts.lib.db.schemas import DEV_PHR, PHR_MASTER
from scripts.lib.identity.base_norm import base_normalize


NO_RESULT_WORDS = {
    "未実施",
    "未受診",
    "実施せず",
    "キャンセル",
    "中止",
    "拒否",
    "対象外",
    "キヤンセル",
}

UNMEASURABLE_WORDS = {
    "測定不能",
    "測定不可",
    "判定不能",
    "検体不良",
    "採血不可",
}

NUMERIC_DATA_TYPES = {"PQ", "INT", "REAL"}
CODE_DATA_TYPES = {"CD", "CO"}
TEXT_DATA_TYPES = {"ST", "TX"}
NUMERIC_LESS_THAN_PATTERN = re.compile(r"^(?:<|＜)\s*([+-]?\d+(?:\.\d+)?)$")
NUMERIC_LESS_THAN_JA_PATTERN = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*(?:未満|以下)$")
UNIT_ALIASES = {
    "": None,
    "%": "%",
    "/MIN": "1/min",
    "1/MIN": "1/min",
    "回/分": "1/min",
    "回／分": "1/min",
    "拍/分": "1/min",
    "拍／分": "1/min",
    "BPM": "1/min",
    "{H`B}/MIN": "1/min",
    "MMHG": "mm[Hg]",
    "MM[HG]": "mm[Hg]",
    "KG": "kg",
    "ＫＧ": "kg",
    "MG/DL": "mg/dL",
    "G/DL": "g/dL",
    "U/L": "U/L",
    "U/I": "U/L",
    "U/ML": "U/mL",
    "IU/L": "U/L",
    "ML/MIN/1.73M2": "ml/min/1.73m2",
    "ML/MIN/1.73M": "ml/min/1.73m2",
    "ML/MIN/1.73M^2": "ml/min/1.73m2",
    "ML/MIN/1.73㎡": "ml/min/1.73m2",
    "ML/MIN/{1.73_M2}": "ml/min/1.73m2",
    "ML/MIN/{1.73M2}": "ml/min/1.73m2",
    "ML/MIN/{1.73_M}": "ml/min/1.73m2",
    "ML/MIN/{1.73M}": "ml/min/1.73m2",
    "ML/MIN./1.73M2": "ml/min/1.73m2",
    "ML/MIN/1.7": "ml/min/1.73m2",
    "ML/分": "ml/min/1.73m2",
    "1/MM3": "1/mm3",
    "/MM3": "1/mm3",
    "/ΜL": "1/mm3",
    "/ΜＬ": "1/mm3",
    "/ΜL": "1/mm3",
    "/UL": "1/mm3",
    "1/UL": "1/mm3",
    "UL": "1/mm3",
    "μL": "1/mm3",
    "µL": "1/mm3",
    "㎕": "1/mm3",
    "/ULITER": "1/mm3",
    "/MICROLITER": "1/mm3",
    "/μL": "1/mm3",
    "/µL": "1/mm3",
    "/㎕": "1/mm3",
    "10*4/MM3": "10*4/mm3",
    "10*4/UL": "10*4/mm3",
    "10*4/μL": "10*4/mm3",
    "10*4/µL": "10*4/mm3",
    "10*4/㎕": "10*4/mm3",
    "×10*4/UL": "10*4/mm3",
    "×10*4/μL": "10*4/mm3",
    "×10*4/µL": "10*4/mm3",
    "×10^4/UL": "10*4/mm3",
    "×10^4/μL": "10*4/mm3",
    "×10^4/µL": "10*4/mm3",
    "万/MM3": "10*4/mm3",
    "万/MM": "10*4/mm3",
    "万/UL": "10*4/mm3",
    "万/μL": "10*4/mm3",
    "万/µL": "10*4/mm3",
    "万/MM^3": "10*4/mm3",
    "10^4/MM3": "10*4/mm3",
    "FL": "fL",
    "ｆｌ": "fL",
    "PG": "pg",
    "PG/ML": "pg/mL",
    "NG/ML": "ng/mL",
}


@dataclass(frozen=True)
class NormalizedExamValue:
    raw_value: str | None
    raw_value_type: str | None
    raw_unit: str | None
    normalized_value: str | None
    normalized_unit: str | None
    nullflavor: str | None
    code_system: str | None
    code_value: str | None
    code_display: str | None
    normalize_status: str
    normalize_reason: str | None
    validation_status: str
    validation_reason: str | None

    def as_exam_item_value_columns(self) -> dict[str, Any]:
        return {
            "raw_value": self.raw_value,
            "raw_value_type": self.raw_value_type,
            "raw_unit": self.raw_unit,
            "normalized_value": self.normalized_value,
            "normalized_unit": self.normalized_unit,
            "nullflavor": self.nullflavor,
            "code_system": self.code_system,
            "code_value": self.code_value,
            "code_display": self.code_display,
            "normalize_status": self.normalize_status,
            "normalize_reason": self.normalize_reason,
            "validation_status": self.validation_status,
            "validation_reason": self.validation_reason,
        }


def _compact_text(value: Any) -> str | None:
    normalized = base_normalize(None if value is None else str(value))
    return normalized


def _raw_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _item_data_type(item: Mapping[str, Any]) -> str | None:
    value = item.get("data_type") or item.get("xml_value_type")
    return _compact_text(value)


def _item_unit(item: Mapping[str, Any]) -> str | None:
    return _compact_text(item.get("unit") or item.get("ucum_unit") or item.get("display_unit"))


def _canonical_unit(unit: str | None) -> str | None:
    value = _compact_text(unit)
    if value is None:
        return None
    key = (
        value.replace("μ", "u")
        .replace("µ", "u")
        .replace("㎕", "uL")
        .replace("㎗", "dL")
        .replace("／", "/")
        .replace("＊", "*")
        .replace("＾", "^")
        .replace("　", "")
        .replace(" ", "")
    )
    return UNIT_ALIASES.get(key.upper(), value)


def _numeric_text(value: str) -> tuple[str, str | None]:
    compact = value.replace(",", "")
    for pattern in (NUMERIC_LESS_THAN_PATTERN, NUMERIC_LESS_THAN_JA_PATTERN):
        match = pattern.match(compact)
        if match:
            return match.group(1), "RAW_VALUE_NUMERIC_COMPARATOR_NORMALIZED"
    return compact, None


def _ok(
    *,
    raw_value: str | None,
    raw_value_type: str | None,
    raw_unit: str | None,
    normalized_value: str | None = None,
    normalized_unit: str | None = None,
    code_system: str | None = None,
    code_value: str | None = None,
    code_display: str | None = None,
    normalize_reason: str | None = None,
) -> NormalizedExamValue:
    return NormalizedExamValue(
        raw_value=raw_value,
        raw_value_type=raw_value_type,
        raw_unit=raw_unit,
        normalized_value=normalized_value,
        normalized_unit=normalized_unit,
        nullflavor=None,
        code_system=code_system,
        code_value=code_value,
        code_display=code_display,
        normalize_status="OK",
        normalize_reason=normalize_reason,
        validation_status="VALID",
        validation_reason=None,
    )


def _skipped(
    *,
    raw_value: str | None,
    raw_value_type: str | None,
    raw_unit: str | None,
    reason: str,
) -> NormalizedExamValue:
    return NormalizedExamValue(
        raw_value=raw_value,
        raw_value_type=raw_value_type,
        raw_unit=raw_unit,
        normalized_value=None,
        normalized_unit=None,
        nullflavor=None,
        code_system=None,
        code_value=None,
        code_display=None,
        normalize_status="SKIPPED",
        normalize_reason=reason,
        validation_status="WARNING",
        validation_reason=reason,
    )


def _error(
    *,
    raw_value: str | None,
    raw_value_type: str | None,
    raw_unit: str | None,
    reason: str,
) -> NormalizedExamValue:
    return NormalizedExamValue(
        raw_value=raw_value,
        raw_value_type=raw_value_type,
        raw_unit=raw_unit,
        normalized_value=None,
        normalized_unit=None,
        nullflavor=None,
        code_system=None,
        code_value=None,
        code_display=None,
        normalize_status="ERROR",
        normalize_reason=reason,
        validation_status="INVALID",
        validation_reason=reason,
    )


def normalize_exam_item_value(
    cur: Any,
    *,
    namecode: str,
    raw_value: Any,
    raw_unit: str | None = None,
    exam_item: Mapping[str, Any] | None = None,
    dev_db: str = DEV_PHR,
    master_db: str = PHR_MASTER,
) -> NormalizedExamValue:
    """Normalize one exam item value for CSV/XML style result insertion."""

    item = exam_item or get_exam_item(cur, namecode, dev_db=dev_db)
    raw_text = _raw_text(raw_value)
    value = _compact_text(raw_value)
    unit = _compact_text(raw_unit)

    if raw_text is None or value is None:
        return _skipped(
            raw_value=None,
            raw_value_type=None,
            raw_unit=unit,
            reason="RAW_VALUE_EMPTY",
        )

    if item is None:
        return _error(
            raw_value=raw_text,
            raw_value_type=None,
            raw_unit=unit,
            reason="EXAM_ITEM_MASTER_NOT_FOUND",
        )

    data_type = _item_data_type(item)
    expected_unit = _item_unit(item)

    if value in NO_RESULT_WORDS:
        return _skipped(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            reason="RAW_VALUE_NO_RESULT",
        )

    if value in UNMEASURABLE_WORDS:
        return _skipped(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            reason="RAW_VALUE_UNMEASURABLE",
        )

    if unit and expected_unit and _canonical_unit(unit) != _canonical_unit(expected_unit):
        return _error(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            reason="UNIT_MISMATCH",
        )

    if data_type in CODE_DATA_TYPES:
        result_code_oid = _compact_text(item.get("result_code_oid"))
        if result_code_oid is None:
            return _error(
                raw_value=raw_text,
                raw_value_type=data_type,
                raw_unit=unit,
                reason="RESULT_CODE_OID_MISSING",
            )
        variant = get_norm_variant(
            cur,
            result_code_oid=result_code_oid,
            raw_value_utf8=raw_text,
            master_db=master_db,
        )
        normalize_reason = "RAW_VALUE_EXACT_MATCH"
        if variant is None and value != raw_text:
            variant = get_norm_variant(
                cur,
                result_code_oid=result_code_oid,
                raw_value_utf8=value,
                master_db=master_db,
            )
            normalize_reason = "RAW_VALUE_NORMALIZED_MATCH"
        if variant is None:
            return _error(
                raw_value=raw_text,
                raw_value_type=data_type,
                raw_unit=unit,
                reason="NORMALIZE_VARIANT_NOT_FOUND",
            )
        return _ok(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            code_system=_compact_text(variant.get("code_system")),
            code_value=_compact_text(variant.get("normalized_code")),
            code_display=_compact_text(variant.get("display_name")),
            normalize_reason=normalize_reason,
        )

    if data_type in NUMERIC_DATA_TYPES:
        numeric_text, normalize_reason = _numeric_text(value)
        try:
            numeric_value = Decimal(numeric_text)
        except InvalidOperation:
            return _error(
                raw_value=raw_text,
                raw_value_type=data_type,
                raw_unit=unit,
                reason="INVALID_VALUE_TYPE",
            )
        return _ok(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            normalized_value=format(numeric_value, "f"),
            normalized_unit=expected_unit or unit,
            normalize_reason=normalize_reason,
        )

    if data_type in TEXT_DATA_TYPES or data_type is None:
        return _ok(
            raw_value=raw_text,
            raw_value_type=data_type,
            raw_unit=unit,
            normalized_value=value,
            normalized_unit=None,
        )

    return _ok(
        raw_value=value,
        raw_value_type=data_type,
        raw_unit=unit,
        normalized_value=value,
        normalized_unit=expected_unit or unit,
    )
