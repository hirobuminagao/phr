# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import unicodedata

# 半角 -> 全角の変換（ASCII範囲）
# 0x21 '!' 〜 0x7E '~' を全角へ（U+FF01〜）
# スペースは別扱いで全角スペースへ
def _to_fullwidth_ascii(s: str) -> str:
    out = []
    for ch in s:
        code = ord(ch)
        if ch == " ":
            out.append("　")  # 全角スペース
        elif 0x21 <= code <= 0x7E:
            out.append(chr(code + 0xFEE0))
        else:
            out.append(ch)
    return "".join(out)

def normalize_insurance_symbol_for_match(value: str | None) -> str:
    """
    保険証記号を照合用に正規化（全角寄せ）
    - 数字も英字も全角へ（例: "A-12" -> "Ａ－１２"）
    - 空白除去（半角/全角）
    - ハイフン類は "－"（全角ハイフン）へ寄せる
    - 文字の互換正規化(NFKC)は “前処理” として使い、その後に全角寄せで確定させる
    """
    if not value:
        return ""

    s = str(value)

    # 互換正規化（例: 半角カナ等の揺れを潰す）
    s = unicodedata.normalize("NFKC", s)

    # 空白除去（半角/全角/タブ等）
    s = re.sub(r"[\s　]+", "", s)

    # いろんなハイフン/長音を一旦 '-' に寄せる（最後に全角化される）
    s = s.replace("－", "-").replace("―", "-").replace("ー", "-").replace("−", "-").replace("‐", "-").replace("-", "-")

    # ASCII範囲を全角へ（数字も英字も全角化）
    s = _to_fullwidth_ascii(s)

    # 最終：ハイフンを “全角ハイフン” に確定（全角化済みだと "－" になってる想定）
    # 念のため他の混入を全部 "－" に寄せる
    s = s.replace("－", "－")

    return s
