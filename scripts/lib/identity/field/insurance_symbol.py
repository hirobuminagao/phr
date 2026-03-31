

# -*- coding: utf-8 -*-

from __future__ import annotations

"""
insurance_symbol.py

保険証記号の正規化・照合用処理

責務:
- raw → field_norm / match / person_id_custom / export を生成
- 用途ごとに保険証記号の canonical 値を分ける
"""

import re

from lib.identity.base_norm import base_normalize
from lib.identity.primitive.convert import to_fullwidth_ascii, to_halfwidth_ascii
from lib.identity.primitive.digits import extract_digits, strip_leading_zeros_keep_zero
from lib.identity.primitive.remove import remove_spaces, remove_symbol_noise
from lib.identity.primitive.normalize import unify_hyphen





def _strip_leading_zero_in_blocks(text: str | None) -> str | None:
    """数字ブロックごとに先頭0を削除する。"""
    if text is None:
        return None

    def repl(m: re.Match[str]) -> str:
        num = m.group(0)
        stripped = num.lstrip("0")
        return stripped if stripped != "" else "0"

    return re.sub(r"\d+", repl, text)



def _needs_fullwidth_export(text: str | None) -> bool:
    """全角文字または非 ASCII 文字を含む場合、export を全角寄せにする。"""
    if text is None:
        return False
    return any(ord(ch) > 0x7F for ch in text)


# ------------------------------------------------------------
# public API
# ------------------------------------------------------------


def normalize_insurance_symbol(raw: str | None) -> dict:
    """insurance_symbol の用途別 canonical 値を生成する。

    v1.1.0 固定方針:

    - field_norm:
      - base_norm を起点にする
      - 空白除去 / 記号ノイズ除去を行う
      - ハイフン類を `-` に統一する
      - 半角 ASCII に寄せる
      - 先頭0は削除しない

    - match:
      - field_norm を起点にする
      - 数字ブロックごとに先頭0を削除する
      - 非数字は残す

    - person_id_custom:
      - field_norm を起点にする
      - 数字のみ抽出する
      - 先頭0を削除する
      - 非数字はすべて落とす

    - export:
      - field_norm を起点にする
      - 全角文字が含まれていれば全角寄せする
      - 半角のみなら field_norm をそのまま使う
    """
    base = base_normalize(raw)

    if base is None:
        return {
            "field_name": "insurance_symbol",
            "raw": raw,
            "base_norm": None,
            "field_norm": None,
            "match": None,
            "person_id_custom": None,
            "export": None,
            "ok": False,
            "missing": True,
            "reason": "missing_raw_or_base_norm",
        }

    # 1. 共通前処理
    normalized = remove_spaces(base)
    normalized = remove_symbol_noise(normalized)
    normalized = unify_hyphen(normalized)
    normalized = to_halfwidth_ascii(normalized)

    if normalized is None or normalized == "":
        return {
            "field_name": "insurance_symbol",
            "raw": raw,
            "base_norm": base,
            "field_norm": None,
            "match": None,
            "person_id_custom": None,
            "export": None,
            "ok": False,
            "missing": True,
            "reason": "empty_after_normalize",
        }

    # 2. field_norm
    field_norm = normalized

    # 3. match
    match = _strip_leading_zero_in_blocks(field_norm)
    if match == "":
        match = None

    # 4. person_id_custom 用
    person_id_digits = extract_digits(field_norm)
    person_id_custom = strip_leading_zeros_keep_zero(person_id_digits)

    # 5. export
    export = to_fullwidth_ascii(field_norm) if _needs_fullwidth_export(base) else field_norm

    return {
        "field_name": "insurance_symbol",
        "raw": raw,
        "base_norm": base,
        "field_norm": field_norm,
        "match": match,
        "person_id_custom": person_id_custom,
        "export": export,
        "ok": True,
        "missing": False,
        "reason": None,
    }