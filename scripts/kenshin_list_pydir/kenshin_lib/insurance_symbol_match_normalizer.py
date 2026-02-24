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
    保険証記号を「人突合キー」として安全側に正規化して返す（v1確定仕様）。

    正規化ルール:
    - None / 空白のみ → ""
    - NFKC 正規化（半角カナ→全角など）
    - 空白削除
    - ハイフン/長音/マイナス類は削除（区切りとして無視）
    - 英字は半角・大文字へ統一
    - 数字は半角へ統一
    - 漢字・カタカナは保持
    - 許可文字: 英字(A-Z) / 数字(0-9) / 漢字 / カタカナ
    - 各「数字ブロック」の先頭ゼロ削除（安全側の誤結合防止）

    例:
      埼００１ → 埼1
      A-001 → A1
      ｶﾀ-0003 → カタ3
      00123 → 123
    """
    if value is None:
        return ""

    s = str(value)

    # ① NFKC（半角カナ→全角など）
    s = unicodedata.normalize("NFKC", s)

    # ② 空白削除
    s = re.sub(r"[\s　]+", "", s)

    # ③ ハイフン/長音/マイナス類削除（区切りとして無視）
    for ch in ("-", "－", "―", "ー", "−", "‐"):
        s = s.replace(ch, "")

    # ④ 英字・数字・漢字・カタカナのみ許可
    #   英数字はASCIIへ、漢字カタカナは保持
    cleaned_chars = []
    for ch in s:
        # ASCII英数字
        if "0" <= ch <= "9" or "A" <= ch <= "Z" or "a" <= ch <= "z":
            cleaned_chars.append(ch)
            continue

        code = ord(ch)
        # 漢字 (CJK Unified Ideographs)
        if 0x4E00 <= code <= 0x9FFF:
            cleaned_chars.append(ch)
            continue
        # カタカナ
        if 0x30A0 <= code <= 0x30FF:
            cleaned_chars.append(ch)
            continue

    s = "".join(cleaned_chars)

    # ⑤ 英字を大文字化（ASCII）
    s = s.upper()

    # ⑥ 数字ブロックごとに先頭ゼロ削除
    # 例: A00012B003 → A12B3
    s = re.sub(r"\d+", lambda m: str(int(m.group())), s)

    return s
