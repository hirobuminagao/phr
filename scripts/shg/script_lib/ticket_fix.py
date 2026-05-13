#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ticket_fix.py

SHG結果XMLの利用券情報 fix 判定ヘルパ。

責務:
- XML側利用券値とDB側利用券値の比較
- fix要否判定
- fix対象フィールド判定
- ticket_fix_status の生成

非責務:
- XMLノード探索
- XML書き換え
- DB接続
- identity生成
- outcome判定
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


TICKET_FIX_STATUS_NO_DIFF = "NO_DIFF"
TICKET_FIX_STATUS_FIXED_REQUIRED = "FIX_REQUIRED"
TICKET_FIX_STATUS_SKIPPED_NO_DB = "SKIPPED_NO_DB"
TICKET_FIX_STATUS_SKIPPED_NO_XML = "SKIPPED_NO_XML"


@dataclass(frozen=True)
class TicketFixResult:
    """利用券fix判定結果。"""

    status: str
    needs_fix: bool
    fix_fields: list[str] = field(default_factory=list)
    xml_ticket_no: str = ""
    xml_ticket_exp: str = ""
    db_ticket_no: str = ""
    db_ticket_exp: str = ""
    reason: str = ""

    def as_csv_columns(self) -> dict[str, str]:
        """CSV出力用の列値へ変換する。"""
        return {
            "ticket_fix_status": self.status,
            "ticket_fix_fields": ",".join(self.fix_fields),
            "ticket_fix_reason": self.reason,
        }


def _to_text(value: Any) -> str:
    """比較用の文字列へ変換する。"""
    if value is None:
        return ""
    return str(value).strip()


def build_ticket_fix_result(
    *,
    xml_ticket_no: Any,
    xml_ticket_exp: Any,
    db_ticket_no: Any,
    db_ticket_exp: Any,
) -> TicketFixResult:
    """XML側利用券値とDB側利用券値を比較し、fix判定結果を返す。

    対象:
    - 利用券整理番号
    - 利用券有効期限

    備考:
    - 汎用XML比較エンジン化しない
    - 利用券/受診券のノード識別は xml_ticket_writer 側の責務
    - ここでは与えられた値同士の比較のみ行う
    """
    xml_no = _to_text(xml_ticket_no)
    xml_exp = _to_text(xml_ticket_exp)
    db_no = _to_text(db_ticket_no)
    db_exp = _to_text(db_ticket_exp)

    if not db_no and not db_exp:
        return TicketFixResult(
            status=TICKET_FIX_STATUS_SKIPPED_NO_DB,
            needs_fix=False,
            xml_ticket_no=xml_no,
            xml_ticket_exp=xml_exp,
            db_ticket_no=db_no,
            db_ticket_exp=db_exp,
            reason="DB側の利用券整理番号・有効期限が不足しているためfix不可",
        )

    if not xml_no and not xml_exp:
        return TicketFixResult(
            status=TICKET_FIX_STATUS_SKIPPED_NO_XML,
            needs_fix=False,
            xml_ticket_no=xml_no,
            xml_ticket_exp=xml_exp,
            db_ticket_no=db_no,
            db_ticket_exp=db_exp,
            reason="XML側の利用券整理番号・有効期限が不足しているため比較不可",
        )

    fix_fields: list[str] = []

    if db_no and xml_no != db_no:
        fix_fields.append("ticket_no")

    if db_exp and xml_exp != db_exp:
        fix_fields.append("ticket_exp")

    if not fix_fields:
        return TicketFixResult(
            status=TICKET_FIX_STATUS_NO_DIFF,
            needs_fix=False,
            xml_ticket_no=xml_no,
            xml_ticket_exp=xml_exp,
            db_ticket_no=db_no,
            db_ticket_exp=db_exp,
            reason="XML値とDB値が一致",
        )

    return TicketFixResult(
        status=TICKET_FIX_STATUS_FIXED_REQUIRED,
        needs_fix=True,
        fix_fields=fix_fields,
        xml_ticket_no=xml_no,
        xml_ticket_exp=xml_exp,
        db_ticket_no=db_no,
        db_ticket_exp=db_exp,
        reason="XML値とDB値に差異あり",
    )
