# -*- coding: utf-8 -*-
"""
数字のみ抽出（digits-only）＋前方0制御のための最小ユーティリティ。

【概要】
本モジュールは、保険証番号に限定せず、任意の文字列から
(a) 数字のみの正規化（digits-only）と
(b) XMLやシステム連携用途に適した前方0の除去（leading-zero stripping）を
提供しつつ、突合用キーとしての役割を果たすことを目的とする。

【目的】
- 異体字・全角半角・記号混在などの表記揺れを吸収する
- DB突合（JOIN / WHERE）で安全に比較できるキーを生成する
- 元データの表示用表現には影響を与えない（非破壊）

【入力】
- value: str | None
  - 保険証番号（生データ）
  - 数値・文字列・None を許容

【出力】
- str
  - 数字のみで構成された文字列
  - 入力が None / 空相当の場合は空文字列を返す

【正規化ルール】
- Unicode 正規化（NFKC）を適用
- 数字（0–9）以外の文字はすべて除去
  - ハイフン、スペース、全角記号等は削除される
- 桁数チェックや妥当性検証は行わない

【想定ユースケース】
- subscribers / medi_xml_ledger 等での保険証番号照合
- person_id_custom 生成前の前処理
- CSV / XML 由来データの突合キー作成

【注意点】
- 表示用・帳票用の番号としては使用しないこと
- 桁数不足・過剰などの異常値もそのまま返る（バリデーション責務外）
- 本関数の仕様変更は、既存データの再正規化を要するため注意
"""
from __future__ import annotations
import unicodedata
import re


def digits_only(value: str | None) -> str:
    """Unicode正規化(NFKC) → 数字(0-9)以外を除去して digits-only を返す。"""
    if not value:
        return ""
    s = unicodedata.normalize("NFKC", str(value))
    return re.sub(r"[^0-9]", "", s)


def strip_leading_zeros(digits: str) -> str:
    """前方0をすべて削除する。全て0/空の場合は空文字列を返す。"""
    if not digits:
        return ""
    s = str(digits)
    s = s.lstrip("0")
    return s


def normalize_insurance_number_for_match(value: str | None) -> str:
    """突合用（match）: digits-only の後、前方0をすべて削除する。

    v1方針: 0001 と 1 を同一として扱うため、match は先頭ゼロを落とす。
    """
    return strip_leading_zeros(digits_only(value))


def normalize_digits_for_xml(value: str | None) -> str:
    """XML/連携用途: digits-only にした後、前方0をすべて削除して返す。"""
    return strip_leading_zeros(digits_only(value))
