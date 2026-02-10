# -*- coding: utf-8 -*-
"""
保険証番号を「照合用キー」として正規化するための最小ユーティリティ。

【概要】
保険証番号（insurance_number）を、データ突合・名寄せ用途に適した
「数字のみの文字列」に正規化する。

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

def normalize_insurance_number_for_match(value: str | None) -> str:
    if not value:
        return ""

    s = unicodedata.normalize("NFKC", str(value))
    # 数字以外を除去
    s = re.sub(r"[^0-9]", "", s)
    return s
