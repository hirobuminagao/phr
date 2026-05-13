

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
outcome_policy.py

SHG outcome 判定の運用ポリシーヘルパ。

責務:
- outcome計算結果を、運用上どのように扱うかを判定する
- finalのみ動機づけ支援など、XML単体では矛盾確定できないケースを除外判定する

非責務:
- XML抽出
- outcome値の計算
- 腹囲体重の実測差分計算
- CSV row生成
- people集約
"""

from __future__ import annotations

from typing import Any


REPORT_CODE_FINAL = "22"

# 腹囲体重は、DB健診時値とXML最終実測値から再判定できるため、
# finalのみ動機づけ支援の除外対象には含めない。
CATEGORY_WAIST_WEIGHT = "腹囲体重"

MOTIVATION_LEVEL_KEYWORDS = (
    "動機づけ支援",
    "動機付け支援",
)


def _to_text(value: Any) -> str:
    """判定用文字列へ変換する。"""
    if value is None:
        return ""
    return str(value).strip()


def is_motivation_guidance(level_text: Any) -> bool:
    """保健指導区分が動機づけ支援系かを判定する。"""
    text = _to_text(level_text)
    if not text:
        return False

    return any(keyword in text for keyword in MOTIVATION_LEVEL_KEYWORDS)


def should_skip_outcome_conflict_for_final_only_motivation(
    *,
    report_code: Any,
    has_initial: bool,
    level_text: Any,
    category: str,
) -> bool:
    """finalのみ動機づけ支援時に、outcome矛盾扱いから除外するかを返す。

    除外条件:
    - report_code = 22
    - initial XML が存在しない
    - 保健指導区分が動機づけ支援
    - 対象カテゴリが腹囲体重以外

    理由:
    動機づけ支援では、計画情報が初回報告側にしか存在しないケースがある。
    そのため、final XML単体では腹囲体重以外の目標と結果の矛盾を確定できない。
    """
    if _to_text(report_code) != REPORT_CODE_FINAL:
        return False

    if has_initial:
        return False

    if not is_motivation_guidance(level_text):
        return False

    if _to_text(category) == CATEGORY_WAIST_WEIGHT:
        return False

    return True


def apply_final_only_motivation_conflict_policy(
    *,
    conflict_result: Any,
    report_code: Any,
    has_initial: bool,
    level_text: Any,
    category: str,
) -> str:
    """finalのみ動機づけ支援の特例を反映した conflict 結果を返す。

    現フェーズでは戻り値を文字列に限定する。
    除外対象の場合は `除外_finalのみ動機づけ` を返し、
    それ以外は元の conflict_result を文字列化して返す。
    """
    if should_skip_outcome_conflict_for_final_only_motivation(
        report_code=report_code,
        has_initial=has_initial,
        level_text=level_text,
        category=category,
    ):
        return "除外_finalのみ動機づけ"

    return _to_text(conflict_result)