from __future__ import annotations

from typing import Any, Dict


from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import to_halfwidth_ascii


# gender 正規化マップ（意味解釈レイヤー）
_MALE_SET = {
    "1",
    "男",
    "男性",
    "m",
    "male",
}

_FEMALE_SET = {
    "2",
    "女",
    "女性",
    "f",
    "female",
}


def normalize_gender_code(raw: str | None) -> Dict[str, Any]:
    """gender_code の field_norm / match を生成する。

    v1.1.0 仕様:

    - base_norm を起点にする
    - 意味解釈により "1"（男） / "2"（女）へ正規化する
    - field_norm と match は同一値
    - 不明値は invalid とする
    """

    base = base_normalize(None if raw is None else str(raw))

    if base is None:
        return {
            "field_name": "gender_code",
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    half = to_halfwidth_ascii(base)

    if half is None:
        return {
            "field_name": "gender_code",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "convert_failed",
        }

    normalized = half.lower()

    if normalized in _MALE_SET:
        return {
            "field_name": "gender_code",
            "raw": raw,
            "base_norm": base,
            "field_norm": "1",
            "match": "1",
            "ok": True,
            "missing": False,
            "reason": None,
        }

    if normalized in _FEMALE_SET:
        return {
            "field_name": "gender_code",
            "raw": raw,
            "base_norm": base,
            "field_norm": "2",
            "match": "2",
            "ok": True,
            "missing": False,
            "reason": None,
        }

    return {
        "field_name": "gender_code",
        "raw": raw,
        "base_norm": base,
        "field_norm": None,
        "match": None,
        "ok": False,
        "missing": True,
        "reason": "invalid_gender_value",
    }