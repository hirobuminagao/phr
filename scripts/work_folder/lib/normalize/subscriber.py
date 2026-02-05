# -*- coding: utf-8 -*-
r"""
加入者(subscriber) 固有の正規化ロジック。
Path: work_folder/phr/lib/normalize/subscribers.py
役割:
- 氏名（漢字/カナ）の分解・正規化
- person_id_custom の生成（生成ロジックは lib 側に委譲）
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict

from phr.lib.errors import NormalizeError
from phr.lib import custom_id_gen


def _hiragana_to_katakana(s: str) -> str:
    out = []
    for ch in s:
        o = ord(ch)
        if 0x3041 <= o <= 0x3096:
            out.append(chr(o + 0x60))
        else:
            out.append(ch)
    return "".join(out)


def _normalize_kana_token(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    return _hiragana_to_katakana(t)


def _normalize_kana_full_no_space(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    t = _hiragana_to_katakana(t)
    t = t.replace("\u3000", " ")
    t = re.sub(r"\s+", "", t)
    return t


def _split_name_by_space(s: str) -> tuple[str, str, str]:
    if not s:
        return ("", "", "")
    t = s.replace("\u3000", " ")
    toks = [tok for tok in re.split(r"\s+", t.strip()) if tok]
    if not toks:
        return ("", "", "")
    if len(toks) == 1:
        return ("", "", toks[0])
    if len(toks) == 2:
        return (toks[0], "", toks[1])
    return (toks[0], " ".join(toks[1:-1]), toks[-1])


def normalize_name_fields(*, kanji_full: str, kana_full: str) -> Dict[str, str]:
    kanji_full = (kanji_full or "").strip()
    kana_full = (kana_full or "").strip()

    if not kana_full:
        raise NormalizeError(
            field="name_kana_full",
            code="required",
            raw_value="",
            message="氏名カナが空です。",
        )

    kfam, kmid, kgiv = _split_name_by_space(kanji_full)
    tfam, tmid, tgiv = _split_name_by_space(kana_full)

    tfam = _normalize_kana_token(tfam)
    tmid = _normalize_kana_token(tmid)
    tgiv = _normalize_kana_token(tgiv)

    full_norm = _normalize_kana_full_no_space(kana_full)

    return {
        "name_kanji_family": kfam,
        "name_kanji_middle": kmid,
        "name_kanji_given": kgiv,
        "name_kana_family": tfam,
        "name_kana_middle": tmid,
        "name_kana_given": tgiv,
        "name_kana_full": full_norm,
    }


def generate_person_id_custom(
    *,
    insurer_number: int,
    insurance_symbol: str,
    insurance_number: str,
    birth_yyyymmdd: str,
) -> str:
    try:
        final_id, _meta = custom_id_gen.generate_id(
            insurer_number=f"{insurer_number:08d}",
            symbol=insurance_symbol or "",
            insurance_number=insurance_number or "",
            birth_yyyymmdd=birth_yyyymmdd or "",
        )
    except Exception as e:
        raise NormalizeError(
            field="person_id_custom",
            code="generate_failed",
            raw_value=f"{insurer_number}/{insurance_symbol}/{insurance_number}/{birth_yyyymmdd}",
            message=f"person_id_custom 生成失敗: {e}",
        )

    if not final_id:
        raise NormalizeError(
            field="person_id_custom",
            code="empty",
            raw_value=f"{insurer_number}/{insurance_symbol}/{insurance_number}/{birth_yyyymmdd}",
            message="person_id_custom が空で返却されました。",
        )

    return final_id
