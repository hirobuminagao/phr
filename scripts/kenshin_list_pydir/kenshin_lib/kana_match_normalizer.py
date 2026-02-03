# -*- coding: utf-8 -*-
"""
kenshin_lib/kana_match_normalizer.py

氏名カナを「照合用」に正規化するための処理。
- 締切優先・事故防止用の最小セット
- 値変換のみを担当（DB非依存）
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
    """
    氏名カナ照合用の正規化

    - NFKC 正規化
    - 全角/半角スペース除去
    - ダッシュ類を長音「ー」に統一
    - 長音の連続を1つに圧縮
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
