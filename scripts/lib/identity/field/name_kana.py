

from __future__ import annotations

from scripts.lib.identity.base_norm import base_normalize
from scripts.lib.identity.primitive.convert import (
    hiragana_to_katakana,
    normalize_small_kana,
    to_fullwidth_ascii,
)
from scripts.lib.identity.primitive.remove import remove_spaces, remove_kana_symbols


def normalize_name_kana_full(raw: str | None) -> dict:
    """name_kana_full の field_norm / match を生成する。

    v1.1.0 仕様:

    - base_norm を起点にする
    - ひらがな → カタカナ
    - 小書き → 大文字
    - スペース除去
    - 中黒・長音・ハイフン等を除去（照合用）
    - 全角カナへ寄せる
    - field_norm と match は同一
    """

    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "name_kana_full",
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    v = hiragana_to_katakana(base)
    v = normalize_small_kana(v)
    v = remove_spaces(v)
    v = remove_kana_symbols(v)
    v = to_fullwidth_ascii(v)

    if v is None or v == "":
        return {
            "field_name": "name_kana_full",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_normalize",
        }

    return {
        "field_name": "name_kana_full",
        "raw": raw,
        "base_norm": base,
        "field_norm": v,
        "match": v,
        "ok": True,
        "missing": False,
        "reason": None,
    }