

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
outcome_point_block_fix.py

SHG結果XMLのアウトカム合計ポイント0 block削除fix。

責務:
- final動機づけ支援に限り、アウトカム合計ポイント0 block削除の要否を判定する
- 90060 section 内の code=1042001060 を持つ entryRelationship を削除対象として特定する
- 共通 XML 削除ヘルパへ削除対象を渡す

非責務:
- XMLファイルの読み込み
- XMLファイルの保存
- 利用券fix
- outcome集計
- people集約
- DB接続
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

from scripts.lib.shg.xml.section_90060_final import (
    find_outcome_total_entry_relationship,
)
from scripts.lib.xml.delete import (
    XmlDeleteResult,
    XmlDeleteTarget,
    delete_xml_element,
)
from scripts.shg.script_lib.outcome_policy import is_motivation_guidance


OUTCOME_TOTAL_POINT_CODE = "1042001060"
OUTCOME_TOTAL_POINT_LABEL = "90060_outcome_total_point_entry_relationship"
OUTCOME_TOTAL_POINT_DELETE_REASON = "final動機づけ支援のアウトカム合計ポイント0 block削除"


@dataclass(frozen=True)
class OutcomePointBlockFixResult:
    """アウトカム合計ポイント0 block削除fix結果。"""

    applied: bool
    status: str
    reason: str
    delete_result: XmlDeleteResult | None = None


def should_delete_outcome_total_point_block(
    *,
    report_code: str | None,
    level_text: str | None,
    outcome_total_points: int | None,
) -> tuple[bool, str]:
    """アウトカム合計ポイント0 block削除fixの実行可否を判定する。

    条件:
    - report_code = 22
    - 保健指導区分が動機づけ支援
    - アウトカム合計ポイント値が 0
    """
    if (report_code or "").strip() != "22":
        return False, "report_codeが22ではありません"

    if not is_motivation_guidance(level_text):
        return False, "保健指導区分が動機づけ支援ではありません"

    if outcome_total_points != 0:
        return False, "アウトカム合計ポイントが0ではありません"

    return True, "削除条件に合致しました"


def apply_outcome_total_point_block_fix(
    *,
    root: ET.Element,
    report_code: str | None,
    level_text: str | None,
    outcome_total_points: int | None,
) -> OutcomePointBlockFixResult:
    """条件に合致する場合、アウトカム合計ポイント0 blockを削除する。

    XML保存は行わない。
    呼び出し元が必要に応じて保存する。
    """
    should_delete, reason = should_delete_outcome_total_point_block(
        report_code=report_code,
        level_text=level_text,
        outcome_total_points=outcome_total_points,
    )

    if not should_delete:
        return OutcomePointBlockFixResult(
            applied=False,
            status="SKIPPED",
            reason=reason,
        )

    location = find_outcome_total_entry_relationship(root)
    if location is None:
        return OutcomePointBlockFixResult(
            applied=False,
            status="TARGET_NOT_FOUND",
            reason=f"code={OUTCOME_TOTAL_POINT_CODE} の entryRelationship が見つかりません",
        )

    delete_result = delete_xml_element(
        XmlDeleteTarget(
            parent=location.parent,
            target=location.target,
            label=OUTCOME_TOTAL_POINT_LABEL,
            reason=OUTCOME_TOTAL_POINT_DELETE_REASON,
        )
    )

    return OutcomePointBlockFixResult(
        applied=delete_result.deleted,
        status=delete_result.status,
        reason=delete_result.message,
        delete_result=delete_result,
    )


def outcome_point_block_fix_result_to_dict(
    result: OutcomePointBlockFixResult,
) -> dict[str, Any]:
    """OutcomePointBlockFixResult をCSV出力などに使いやすい dict へ変換する。"""
    delete_result = result.delete_result

    return {
        "outcome_point_block_fix_applied": result.applied,
        "outcome_point_block_fix_status": result.status,
        "outcome_point_block_fix_reason": result.reason,
        "outcome_point_block_delete_label": delete_result.label if delete_result else "",
        "outcome_point_block_delete_reason": delete_result.reason if delete_result else "",
        "outcome_point_block_delete_message": delete_result.message if delete_result else "",
    }