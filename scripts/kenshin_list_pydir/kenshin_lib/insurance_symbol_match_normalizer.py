# -*- coding: utf-8 -*-
"""
insurance_symbol_match_normalizer.py

【目的】
保険証記号（insurance_symbol）を「照合用キー」として比較できる形に正規化する。

【正規化方針（固定仕様）】
- 入力が None / 空文字 / 空白のみの場合は "" を返す。
- 互換正規化（NFKC）で表記ゆれ（半角カナ等）を前処理で潰す。
- 空白（半角/全角/タブ等）はすべて除去する。
- ハイフン/長音/マイナス等の類似文字は、最終的に「全角ハイフン（－）」へ寄せる。
- ASCII 範囲（0x21〜0x7E）の英数字・記号は全角へ寄せる（例: "A-12" -> "Ａ－１２"）。

【主な用途】
- 受領データやXML由来の保険証記号を、DB上の照合列（*_match）へ格納する際の正規化。
- JOIN/突合/重複検出など、比較用途に限定して使用する（表示用ではない）。

【注意】
- 本モジュールは「照合の安定性」を最優先し、入力の意味解釈（例: 部署コード等の推測）は行わない。
- 仕様変更は、必ず影響範囲（既存データの再正規化要否、JOIN結果の変化）を確認した上で行う。
"""
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
    保険証記号を照合用に正規化して返す。

    例:
      - "A-12"      -> "Ａ－１２"
      - " 埼-30 "   -> "埼－３０"
      - "ﾊﾝｶｸ-1"    -> "ハンカク－１" （NFKCの後に全角寄せ）

    戻り値:
      - 入力が None/空の場合: ""
      - それ以外: 正規化済み文字列

    実装メモ:
      - NFKCは前処理として使用し、その後に空白除去・ハイフン統一・全角化で確定させる。
    """
    if not value:
        return ""

    s = str(value)

    # 互換正規化（例: 半角カナ等の揺れを潰す）
    s = unicodedata.normalize("NFKC", s)

    # 空白除去（半角/全角/タブ等）
    s = re.sub(r"[\s　]+", "", s)

    # いろんなハイフン/長音/マイナスを一旦 "-" に寄せる（最後に全角化される）
    for ch in ("－", "―", "ー", "−", "‐"):
        s = s.replace(ch, "-")

    # ASCII範囲を全角へ（数字も英字も全角化）
    s = _to_fullwidth_ascii(s)

    # 最終：ハイフンを「全角ハイフン（－）」に確定
    # 念のため他の混入を全部 "－" に寄せる
    s = s.replace("－", "－")

    return s
