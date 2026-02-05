# phr/lib/text_basic.py
from __future__ import annotations
import unicodedata
import re


def to_half_digits(s: str) -> str:
    """全角数字→半角数字"""
    if s is None:
        return ""
    return unicodedata.normalize("NFKC", str(s))


def digits_only(s: str) -> str:
    """文字列中の数字だけ抽出"""
    if not s:
        return ""
    return "".join(ch for ch in to_half_digits(s) if ch.isdigit())


def split_digit_chunks(s: str) -> list[str]:
    """任意区切り値を → [数字ブロック, ...]"""
    src = to_half_digits(s)
    return [p for p in re.split(r"[^\d]+", src) if p]
