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
    """birthdate の field_norm / match を生成する。

    v1.1.0 仕様:

    - `base_norm` を起点にする
    - 数字抽出後の値を `dates` helper で解釈する
    - 西暦8桁 (`yyyymmdd`) と元号コード7桁 (`era_code_7`) を受け付ける
    - `field_norm` は `YYYY-MM-DD`
    - `match` は `YYYYMMDD`
    - 解釈できない値は invalid とする
    """
    # --- fast path: MySQL DATE (datetime.date) ---
    if isinstance(raw, date):
        year, month, day = raw.year, raw.month, raw.day
        field_norm = to_yyyy_mm_dd(year, month, day)
        match = to_yyyymmdd(year, month, day)
        return {
            "field_name": "birthdate",
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
            "field_name": "birthdate",
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
            "field_name": "birthdate",
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
            "field_name": "birthdate",
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
            "field_name": "birthdate",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "invalid_birthdate_value",
        }

    year, month, day = parsed
    field_norm = to_yyyy_mm_dd(year, month, day)
    match = to_yyyymmdd(year, month, day)

    return {
        "field_name": "birthdate",
        "raw": raw,
        "base_norm": base,
        "field_norm": field_norm,
        "match": match,
        "ok": True,
        "missing": False,
        "reason": None,
    }
