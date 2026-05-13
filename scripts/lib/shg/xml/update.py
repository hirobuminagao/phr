

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update.py

SHG XML の既存値更新ヘルパ。

責務:
- 値取得層が返した location（element / attribute）を使って既存XML値を更新する
- 更新結果を dict で返す
- 更新後のXMLを指定パスへ保存する
- 渡された更新値をそのまま既存XML属性へ反映する

非責務:
- XML上の値位置を探す
- 利用券 / 受診券などの業務判断
- 修正対象にするかどうかの判定
- 更新値の形式チェック
- 利用券整理番号の桁数チェック
- 利用券有効期限のDATE→yyyymmdd変換
- 新規XMLブロック作成
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from lxml import etree as LET


CDA_NS = "urn:hl7-org:v3"

STATUS_UPDATED = "UPDATED"
STATUS_NO_CHANGE = "NO_CHANGE"
STATUS_LOCATION_MISSING = "LOCATION_MISSING"
STATUS_ERROR = "ERROR"


def _to_text(value: Any) -> str:
    """XML属性へ設定するための文字列へ変換する。"""
    if value is None:
        return ""
    return str(value).strip()


def update_xml_value(
    *,
    item_name: str,
    location: Any,
    new_value: Any,
) -> dict[str, Any]:
    """指定locationのXML属性値を更新する。

    Args:
        item_name: 更新対象項目名。
        location: 値取得層が返した location。
            以下の属性を持つ前提:
            - elem: XML element
            - attr_name: str
        new_value: 更新後の値。
            呼び出し側で正規化・形式変換済みの値を渡す前提。

    Returns:
        dict[str, Any]:
            - ok
            - status
            - item_name
            - old_value
            - new_value
            - message
    """
    if location is None:
        return {
            "ok": False,
            "status": STATUS_LOCATION_MISSING,
            "item_name": item_name,
            "old_value": "",
            "new_value": _to_text(new_value),
            "message": "更新場所が指定されていない",
        }

    elem = getattr(location, "elem", None)
    attr_name = getattr(location, "attr_name", None)

    if elem is None or not attr_name:
        return {
            "ok": False,
            "status": STATUS_LOCATION_MISSING,
            "item_name": item_name,
            "old_value": "",
            "new_value": _to_text(new_value),
            "message": "location に elem または attr_name が存在しない",
        }

    old_value = _to_text(elem.get(attr_name))
    next_value = _to_text(new_value)

    if old_value == next_value:
        return {
            "ok": True,
            "status": STATUS_NO_CHANGE,
            "item_name": item_name,
            "old_value": old_value,
            "new_value": next_value,
            "message": "既に同じ値のため更新不要",
        }

    elem.set(attr_name, next_value)

    return {
        "ok": True,
        "status": STATUS_UPDATED,
        "item_name": item_name,
        "old_value": old_value,
        "new_value": next_value,
        "message": "XML値を更新",
    }


def save_xml(
    *,
    xml_path: Path,
    root: LET._Element,
) -> dict[str, Any]:
    """XMLを指定パスへ保存する。

    既存XMLの保存のみを行う。新規XMLブロック作成は行わない。
    """
    try:
        tree = LET.ElementTree(root)
        tree.write(
            str(xml_path),
            encoding="utf-8",
            xml_declaration=True,
        )

    except Exception as exc:
        return {
            "ok": False,
            "status": STATUS_ERROR,
            "item_name": "",
            "old_value": "",
            "new_value": "",
            "message": f"XML保存に失敗: {exc}",
        }

    return {
        "ok": True,
        "status": STATUS_UPDATED,
        "item_name": "",
        "old_value": "",
        "new_value": "",
        "message": "XMLを保存",
    }