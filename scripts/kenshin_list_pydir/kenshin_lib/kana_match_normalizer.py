# -*- coding: utf-8 -*-
"""
kenshin_lib/kana_match_normalizer.py

【目的】
氏名カナ等の文字列を、突合（JOIN/名寄せ）に使える「安定した照合キー」表現に正規化する。
純粋変換のみを扱い、DBアクセスや副作用は持たない。

【正規化ルール】
- 入力が None の場合は ""（空文字）を返す
- 文字列化して前後の空白を strip
- Unicode 正規化（NFKC）を適用
  - 例: 半角カナ→全角カナ、全角英数→半角英数 など
- 半角スペース/全角スペースを除去
- 各種ダッシュ/ハイフン/マイナス類を長音符「ー」に統一
- 半角長音符「ｰ」を長音符「ー」に統一
- 長音符の連続を 1 つに圧縮（例: "ーー" → "ー"）

【注意】
- これは表示用の整形ではなく、あくまで照合（マッチング）用のキー生成を目的とする。
- 出力が「正しいカナ」であることは保証しない（照合の安定性を優先）。
"""

from __future__ import annotations

import unicodedata

# ダッシュ類 → 長音
_DASH_CHARS = {
    "\u2015",  # ―
    "\u2212",  # −
    "\u2010",  # ‐
    "\u2011",  # -
    "\u2012",  # ‒
    "\u2013",  # –
    "\u2014",  # —
}
_LONG_VOWEL = "ー"


def normalize_kana_for_match(value: str | None) -> str:
    """照合用にカナ文字列を正規化して返す。

    Args:
        value: 元の氏名カナ文字列。None 可。

    Returns:
        突合（JOIN/名寄せ）に使える決定的な正規化文字列。

    正規化の詳細はモジュールdocstringを参照。
    """
    if value is None:
        return ""

    s = str(value).strip()

    # 全角英数・半角カナなどを統一
    s = unicodedata.normalize("NFKC", s)

    # スペース除去
    s = s.replace(" ", "").replace("　", "")

    # ダッシュ → 長音
    for ch in _DASH_CHARS:
        s = s.replace(ch, _LONG_VOWEL)

    # まれな長音記号
    s = s.replace("ｰ", _LONG_VOWEL)

    # 長音連続を圧縮
    while _LONG_VOWEL * 2 in s:
        s = s.replace(_LONG_VOWEL * 2, _LONG_VOWEL)

    return s
