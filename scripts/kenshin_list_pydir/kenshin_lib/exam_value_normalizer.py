# -*- coding: utf-8 -*-
"""
kenshin_lib/exam_value_normalizer.py

健診結果 raw_value を、exam_item_master.xml_value_type に基づいて正規化する。

- CD: norm_variants を引く（result_code_oid + raw_token_norm -> normalized_code）
- PQ: 数値パース（全角/カンマ/単位混在などを吸収）
- ST/CO: 文字列トリム＋軽い正規化

戻り値:
  (status, normalized_value, normalized_code, normalized_unit, error_reason)

status:
  OK / EMPTY / UNPARSABLE / NO_MASTER / NO_DICT
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class NormResult:
    status: str
    normalized_value: Optional[str] = None   # PQ/ST/CO
    normalized_code: Optional[str] = None    # CD
    normalized_unit: Optional[str] = None    # PQ
    error_reason: Optional[str] = None


# -------------------------
# small utils
# -------------------------
def _s(v: Optional[str]) -> str:
    return "" if v is None else str(v)


def _strip(v: Optional[str]) -> str:
    return _s(v).strip()


def _to_halfwidth_basic(s: str) -> str:
    # まずは安全な範囲だけ: 全角数字/記号を半角へ
    tbl = str.maketrans({
        "０":"0","１":"1","２":"2","３":"3","４":"4","５":"5","６":"6","７":"7","８":"8","９":"9",
        "．":".","，":",","＋":"+","－":"-","ー":"-","−":"-",
        "％":"%","／":"/","　":" ",
    })
    return s.translate(tbl)


def _compact_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


# -------------------------
# CD token normalize
# -------------------------
def normalize_cd_token(raw: Optional[str]) -> str:
    """
    norm_variants の raw_token_norm に合わせるための前処理。
    ここは「辞書のキー生成」なので ASCII に寄せる。
    """
    s = _compact_spaces(_to_halfwidth_basic(_strip(raw))).upper()

    # よくある表記揺れを寄せる（必要なら後で追加）
    s = s.replace("＋", "+")
    s = s.replace("PLUS", "+")
    s = s.replace("POSITIVE", "POS")
    s = s.replace("NEGATIVE", "NEG")

    # "2+" "++" などはそのまま残す（辞書側で吸収）
    return s


# -------------------------
# PQ parse
# -------------------------
_NUM_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")

def normalize_pq(raw: Optional[str]) -> NormResult:
    """
    PQ:
    - 全角->半角
    - 桁カンマ除去
    - 値と単位がくっついてても数値だけ拾う
    """
    s0 = _strip(raw)
    if s0 == "":
        return NormResult(status="EMPTY")

    s = _compact_spaces(_to_halfwidth_basic(s0))

    # よくある「測定不能」系（必要なら後で追加）
    if s in ("-", "ー", "―", "×", "＊", "*"):
        return NormResult(status="UNPARSABLE", error_reason="SYMBOL_ONLY")

    # 1) 桁区切りカンマを除去（"1,234"）
    s = re.sub(r"(?<=\d),(?=\d{3}\b)", "", s)

    # 2) まず数値を拾う
    m = _NUM_RE.search(s)
    if not m:
        return NormResult(status="UNPARSABLE", error_reason="NOT_NUMERIC")

    num = m.group(0)

    # 3) 単位らしきもの（数字以降の末尾）を軽く拾う（空でもOK）
    tail = s[m.end():].strip()
    unit = tail if tail else None

    return NormResult(status="OK", normalized_value=num, normalized_unit=unit)


# -------------------------
# ST/CO normalize
# -------------------------
def normalize_text(raw: Optional[str]) -> NormResult:
    s0 = _strip(raw)
    if s0 == "":
        return NormResult(status="EMPTY")
    s = _compact_spaces(_to_halfwidth_basic(s0))
    return NormResult(status="OK", normalized_value=s)


def normalize_by_type(xml_value_type: Optional[str], raw: Optional[str]) -> NormResult:
    t = _strip(xml_value_type).upper()
    if t == "":
        return NormResult(status="NO_MASTER", error_reason="NO_XML_VALUE_TYPE")

    if t == "CD":
        tok = normalize_cd_token(raw)
        if tok == "":
            return NormResult(status="EMPTY")
        # CD は辞書引きが必要なのでここでは token だけ返す（normalized_code は後で埋める）
        return NormResult(status="OK", normalized_value=tok)  # normalized_value に token を載せる

    if t == "PQ":
        return normalize_pq(raw)

    if t in ("ST", "CO"):
        return normalize_text(raw)

    # 想定外タイプは text と同等扱い（落とさない）
    r = normalize_text(raw)
    if r.status == "OK":
        return NormResult(status="OK", normalized_value=r.normalized_value, error_reason="UNKNOWN_TYPE_FALLBACK")
    return r
