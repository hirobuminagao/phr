

from __future__ import annotations

from lib.identity.base_norm import base_normalize
from lib.identity.primitive.digits import (
    extract_digits,
    has_max_length,
    strip_leading_zeros_keep_zero,
)


def normalize_insurer_number(raw: str | None) -> dict:
    """insurer_number の field_norm / match を生成する。

    v1.1.0 の現在仕様では、insurer_number は以下の方針とする。

    - `base_norm` を起点にする
    - 数字以外は除去する
    - 先頭0は削除する
    - 全て0の場合は `"0"` とする
    - 正規化後の値が 8 桁を超える場合は invalid とする
    - `field_norm` と `match` は同一値とする
    """
    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "insurer_number",
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
            "field_name": "insurer_number",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_digits",
        }

    norm = strip_leading_zeros_keep_zero(digits_only)
    if norm is None:
        return {
            "field_name": "insurer_number",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_digits",
        }

    if not has_max_length(norm, 8):
        return {
            "field_name": "insurer_number",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "invalid_too_long",
        }

    return {
        "field_name": "insurer_number",
        "raw": raw,
        "base_norm": base,
        "field_norm": norm,
        "match": norm,
        "ok": True,
        "missing": False,
        "reason": None,
    }