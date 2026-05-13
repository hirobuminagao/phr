#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xml_ticket_writer.py

SHG結果XMLの利用券fix適用ヘルパ。

責務:
- TicketFixResult を受け取り、利用券XML更新へ橋渡しする
- 利用券整理番号・利用券有効期限の location を basic.py から取得する
- 共通 update.py を使って既存XML属性値を更新する
- 必要に応じて更新済みXMLを同じ展開済みXMLパスへ保存する

非責務:
- 利用券値の比較
- fix要否判定
- DB接続
- identity生成
- outcome判定
- 利用券participant探索条件の再実装
- XML属性値の直接更新処理
- 新規XMLブロック作成
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from scripts.lib.shg.xml.basic import TicketInfo, get_ticket_info
from scripts.lib.shg.xml.update import save_xml, update_xml_value
from scripts.shg.script_lib.ticket_fix import TicketFixResult


FIELD_TICKET_NO = "ticket_no"
FIELD_TICKET_EXP = "ticket_exp"

ITEM_NAME_TICKET_NO = "利用券整理番号"
ITEM_NAME_TICKET_EXP = "利用券有効期限"


@dataclass(frozen=True)
class XmlTicketWriteResult:
    """利用券XML更新結果。"""

    updated: bool
    updated_fields: list[str]
    reason: str
    update_results: list[dict[str, Any]]
    save_result: dict[str, Any] | None = None


def _build_update_targets(
    ticket_info: TicketInfo,
    ticket_fix_result: TicketFixResult,
) -> list[tuple[str, Any, str]]:
    """TicketFixResult から update_xml_value 用の更新対象を作る。

    Returns:
        list[tuple[item_name, location, new_value]]
    """
    targets: list[tuple[str, Any, str]] = []

    if FIELD_TICKET_NO in ticket_fix_result.fix_fields:
        targets.append(
            (
                ITEM_NAME_TICKET_NO,
                ticket_info.ticket_no_location,
                ticket_fix_result.db_ticket_no,
            )
        )

    if FIELD_TICKET_EXP in ticket_fix_result.fix_fields:
        targets.append(
            (
                ITEM_NAME_TICKET_EXP,
                ticket_info.ticket_exp_location,
                ticket_fix_result.db_ticket_exp,
            )
        )

    return targets


def update_usage_ticket_node(
    root: ET.Element,
    ticket_fix_result: TicketFixResult,
) -> XmlTicketWriteResult:
    """利用券情報を ticket_fix_result のDB値で更新する。

    XML保存は行わず、rootをインメモリで更新する。
    """
    if not ticket_fix_result.needs_fix:
        return XmlTicketWriteResult(
            updated=False,
            updated_fields=[],
            reason="fix不要",
            update_results=[],
        )

    ticket_info = get_ticket_info(root)
    targets = _build_update_targets(ticket_info, ticket_fix_result)

    if not targets:
        return XmlTicketWriteResult(
            updated=False,
            updated_fields=[],
            reason="更新対象フィールドなし",
            update_results=[],
        )

    update_results: list[dict[str, Any]] = []
    updated_fields: list[str] = []
    failed_messages: list[str] = []

    for item_name, location, new_value in targets:
        result = update_xml_value(
            item_name=item_name,
            location=location,
            new_value=new_value,
        )
        update_results.append(result)

        if result.get("status") == "UPDATED":
            updated_fields.append(str(result.get("item_name") or item_name))
        elif not result.get("ok"):
            failed_messages.append(f"{item_name}: {result.get('message', '')}")

    if failed_messages:
        return XmlTicketWriteResult(
            updated=bool(updated_fields),
            updated_fields=updated_fields,
            reason="; ".join(failed_messages),
            update_results=update_results,
        )

    return XmlTicketWriteResult(
        updated=bool(updated_fields),
        updated_fields=updated_fields,
        reason="DB値で利用券情報を更新" if updated_fields else "更新不要",
        update_results=update_results,
    )


def update_usage_ticket_xml_file(
    *,
    xml_path: Path,
    root: ET.Element,
    ticket_fix_result: TicketFixResult,
) -> XmlTicketWriteResult:
    """利用券情報を更新し、更新があれば同じXMLパスへ保存する。"""
    write_result = update_usage_ticket_node(
        root=root,
        ticket_fix_result=ticket_fix_result,
    )

    if not write_result.updated:
        return write_result

    save_result = save_xml(
        xml_path=xml_path,
        root=root,
    )

    return XmlTicketWriteResult(
        updated=write_result.updated,
        updated_fields=write_result.updated_fields,
        reason=write_result.reason if save_result.get("ok") else str(save_result.get("message", "XML保存に失敗")),
        update_results=write_result.update_results,
        save_result=save_result,
    )