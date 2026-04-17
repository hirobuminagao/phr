def normalize_date_to_ymd_and_compact(raw: str | date | None, *, purpose: str) -> dict:
    """汎用日付正規化（目的ベース）。

    - purpose: フィールド名ではなく用途名（例: birthdate, exam_date など）
    - field_norm: YYYY-MM-DD
    - match: YYYYMMDD
    """

    # --- fast path: MySQL DATE (datetime.date) ---
    if isinstance(raw, date):
        year, month, day = raw.year, raw.month, raw.day
        field_norm = to_yyyy_mm_dd(year, month, day)
        match = to_yyyymmdd(year, month, day)
        return {
            "field_name": purpose,
            "raw": raw,
            "base_norm": None,
            "field_norm": field_norm,
            "match": match,
            "ok": True,
            "missing": False,
            "reason": None,
        }

    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": purpose,
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    digits_only = extract_digits(base)
    if digits_only is None:
        return {
            "field_name": purpose,
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_digits",
        }

    fmt = detect_date_format(digits_only)
    if fmt is None:
        return {
            "field_name": purpose,
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "invalid_date_format",
        }

    parsed: tuple[int, int, int] | None
    if fmt == "yyyymmdd":
        parsed = parse_yyyymmdd(digits_only)
    elif fmt == "era_code_7":
        parsed = parse_era_code_7(digits_only)
    else:
        parsed = None

    if parsed is None:
        return {
            "field_name": purpose,
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "invalid_date_value",
        }

    year, month, day = parsed
    field_norm = to_yyyy_mm_dd(year, month, day)
    match = to_yyyymmdd(year, month, day)

    return {
        "field_name": purpose,
        "raw": raw,
        "base_norm": base,
        "field_norm": field_norm,
        "match": match,
        "ok": True,
        "missing": False,
        "reason": None,
    }
from __future__ import annotations

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.digits import extract_digits
from scripts.lib.identity.primitive.dates import (
    detect_date_format,
    parse_era_code_7,
    parse_yyyymmdd,
    to_yyyy_mm_dd,
    to_yyyymmdd,
)
from datetime import date


def normalize_birthdate(raw: str | date | None) -> dict:
    """birthdate 用ラッパー（既存互換）。"""
    return normalize_date_to_ymd_and_compact(raw, purpose="birthdate")
