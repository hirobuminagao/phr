# phr/lib/normalize/rules.py
# -*- coding: utf-8 -*-
r"""
normalize/rules.py — fund 加入者CSV向け：列ごとの正規化ルール定義（唯一窓口）

Path   : scripts/work_folder/lib/normalize/rules.py
Project: PHR / work_folder/phr

Purpose:
    - fund 側の加入者CSV（staging_fund 取込）で使う「列→内部キー」の正規化ルールを集約する。
    - import 側はこのモジュールに依存し、列ごとの正規化処理を 1 行に合成する。

Design (v1.0 as-is):
    - 正規化ロジック本体は normalize/common.py に集約（このファイルは合成とI/O定義のみ）
    - 例外は NormalizeError のみを投げる（握り潰さない／呼び出し側で行スキップ判断）
    - ルール関数は "入力row" から "部分dict" を返し、呼び出し側で merge して1行を構築する

V1.0 Freeze (Scope / Contract):
    - Inputs:
        - row: Dict[str, Any]（キー名は import 側で MAP 済みの内部キーを想定）
    - Outputs:
        - 各 rule_* は staging に流し込む列の部分dictを返す（None も可。DATE列は ISO 'YYYY-MM-DD'）
    - Idempotency:
        - 本モジュールは状態を持たない（pure）。同じ入力rowから同じ出力を返す
    - Non-goals:
        - CSVのヘッダマッピング、DB INSERT、run/err 記帳（呼び出し側の責務）
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
    v1.0: digits-only 強制はしない（表記ゆれ低減 + 数字連結の補助値を返す）
    """
    s_norm, digits_val = normalize_insurance_symbol(row.get("insurance_symbol", ""))
    return {
        "insurance_symbol": s_norm,
        "insurance_symbol_digits": digits_val,
    }


def rule_insurance_number(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: row['insurance_number']
    出力: insurance_number（必須・digits-only）
    v1.0: 桁数固定はしない（妥当性は下流の突合仕様で扱う）
    """
    return {
        "insurance_number": normalize_insurance_number_required(
            row.get("insurance_number", ""),
            field="insurance_number",
            # v1.0: src/line_no 等の文脈情報は import 側で持つ（NormalizeError に付与する拡張は将来）
        )
    }


def rule_insurance_branchnumber(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: row['insurance_branchnumber']
    出力: insurance_branchnumber（任意。空は None）
    """
    return {
        "insurance_branchnumber": normalize_branchnumber_optional(
            row.get("insurance_branchnumber", "")
        )
    }


def rule_qualification_dates(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    入力: 開始日/終了日（表記ゆれあり）
    出力: ISO 'YYYY-MM-DD'（MySQL DATE に入れる前提）
    v1.0: 空は None。形式推定に失敗したら NormalizeError
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


# ルールセット（import 側はこれを順に適用し、dict merge して1行を作る）
FUND_SUBSCRIBER_RULES: list[RuleFn] = [
    rule_insurance_symbol,
    rule_insurance_number,
    rule_insurance_branchnumber,
    rule_qualification_dates,
]
