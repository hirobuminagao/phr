# -*- coding: utf-8 -*-
r"""
normalize/subscriber.py — 加入者（subscriber）固有の正規化ロジック

Path   : scripts/work_folder/lib/normalize/subscriber.py
Project: PHR / work_folder/phr

Purpose:
    - 氏名（漢字/カナ）の分解・正規化
    - person_id_custom 生成のラッパー（実ロジックは lib/custom_id_gen に委譲）

Design (v1.0 as-is):
    - 氏名カナは必須（空の場合は NormalizeError）
    - カナは NFKC 正規化 → ひらがな→カタカナ変換 → 空白整理
    - full は「空白除去済みカナ」を保持（照合キー用途を想定）
    - person_id_custom は custom_id_gen.generate_id を呼び出し、例外を NormalizeError に包む

V1.0 Freeze (Scope / Contract):
    - Inputs:
        - kanji_full: 漢字氏名（空可）
        - kana_full : カナ氏名（必須）
    - Outputs:
        - name_kanji_family/middle/given
        - name_kana_family/middle/given
        - name_kana_full（空白除去済み・全角カタカナ）
        - person_id_custom（別関数）
    - Error policy:
        - カナ未入力は NormalizeError(required)
        - person_id_custom 生成失敗は NormalizeError(generate_failed/empty)
    - Non-goals:
        - 名寄せロジック（同一人物判定）、漢字の表記揺れ補正、DB更新処理
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict

from phr.lib.errors import NormalizeError
from phr.lib import custom_id_gen

 # v1.0: ひらがな→カタカナ単純変換（コードポイント+0x60）
 # - 長音符や濁点結合の高度補正は行わない
def _hiragana_to_katakana(s: str) -> str:
    out = []
    for ch in s:
        o = ord(ch)
        if 0x3041 <= o <= 0x3096:
            out.append(chr(o + 0x60))
        else:
            out.append(ch)
    return "".join(out)

 # v1.0: トークン単位のカナ正規化（NFKC → ひらがな→カタカナ）
def _normalize_kana_token(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    return _hiragana_to_katakana(t)

 # v1.0: フルカナの正規化（空白除去版）
 # - 照合キー用途を想定し、連結済み文字列を返す
def _normalize_kana_full_no_space(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "")
    t = _hiragana_to_katakana(t)
    t = t.replace("\u3000", " ")
    t = re.sub(r"\s+", "", t)
    return t

 # v1.0: 氏名を空白で分割（family / middle / given）
 # - 3トークン以上は middle にまとめる
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

    # v1.0: 氏名カナは必須（照合キー生成の前提）
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

 # v1.0: person_id_custom 生成ラッパー
 # - custom_id_gen.generate_id を呼び出し、例外を NormalizeError に包む
 # - insurer_number は 8桁ゼロ埋め文字列にして渡す
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
