# phr/lib/normalize/rules.py
# -*- coding: utf-8 -*-
r"""
加入者CSV（fund）向け：列ごとの正規化ルール定義（唯一窓口）

方針:
- 正規化ロジック本体は normalize/common.py に集約
- この rules.py は「入力→出力」の対応と合成だけを持つ
- 例外は NormalizeError のみを投げる（握り潰さない）
"""

from __future__ import annotations

from typing import Any, Dict, Callable

from phr.lib.normalize.common import (
    normalize_insurance_symbol,
    normalize_insurance_number_required,
    normalize_branchnumber_optional,
    normalize_date_iso,
)

RuleFn = Callable[[Dict[str, Any]], Dict[str, Any]]


def rule_insurance_symbol(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: row['insurance_symbol']
    出力: insurance_symbol, insurance_symbol_digits
    """
    s_norm, digits_val = normalize_insurance_symbol(row.get("insurance_symbol", ""))
    return {
        "insurance_symbol": s_norm,
        "insurance_symbol_digits": digits_val,
    }


def rule_insurance_number(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: row['insurance_number']
    出力: insurance_number（必須・数字のみ）
    """
    return {
        "insurance_number": normalize_insurance_number_required(
            row.get("insurance_number", ""),
            field="insurance_number",
            # src/line_no は import 側で流し込めるように後で拡張する
        )
    }


def rule_insurance_branchnumber(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: row['insurance_branchnumber']
    出力: insurance_branchnumber（任意）
    """
    return {
        "insurance_branchnumber": normalize_branchnumber_optional(
            row.get("insurance_branchnumber", "")
        )
    }


def rule_qualification_dates(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: 開始日/終了日（表記揺れあり）
    出力: ISO 'YYYY-MM-DD'（MySQL DATE に入れる前提）
    """
    return {
        "qualification_start_date": normalize_date_iso(
            row.get("qualification_start_date"),
            field="qualification_start_date",
        ),
        "qualification_end_date": normalize_date_iso(
            row.get("qualification_end_date"),
            field="qualification_end_date",
        ),
    }


# ルールセット（import 側はこれを順に適用して1行を作る）
FUND_SUBSCRIBER_RULES: list[RuleFn] = [
    rule_insurance_symbol,
    rule_insurance_number,
    rule_insurance_branchnumber,
    rule_qualification_dates,
]
