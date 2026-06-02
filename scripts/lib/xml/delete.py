

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
delete.py

XML共通削除ヘルパ。

責務:
- 呼び出し元が特定済みの XML Element を parent から削除する
- 削除結果を構造化して返す

非責務:
- XMLファイルの読み込み
- XMLファイルの保存
- XPath探索
- 削除条件の判定
- SHG / 健診などの業務判定
- OID / section / entry / observation の意味判定
- XML属性値の更新
- 新規XML要素の作成
- 空になった親要素の自動削除
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET


@dataclass(frozen=True)
class XmlDeleteTarget:
    """XML削除対象。

    parent:
        削除対象要素の親要素。
    target:
        削除対象要素。parent の直接の子要素であること。
    label:
        呼び出し元が指定する識別ラベル。
    reason:
        呼び出し元が指定する削除理由。
    """

    parent: ET.Element | None
    target: ET.Element | None
    label: str = ""
    reason: str = ""


@dataclass(frozen=True)
class XmlDeleteResult:
    """XML削除結果。"""

    deleted: bool
    label: str
    reason: str
    message: str
    status: str


def delete_xml_element(target: XmlDeleteTarget) -> XmlDeleteResult:
    """指定されたXML要素を親要素から削除する。

    削除条件の判定は行わない。
    呼び出し元が削除対象として確定した parent / target のみを受け取る。

    Args:
        target: 削除対象情報。

    Returns:
        XmlDeleteResult: 削除結果。
    """
    label = target.label
    reason = target.reason

    if target.parent is None:
        return XmlDeleteResult(
            deleted=False,
            label=label,
            reason=reason,
            status="PARENT_MISSING",
            message="削除対象の親要素が指定されていません",
        )

    if target.target is None:
        return XmlDeleteResult(
            deleted=False,
            label=label,
            reason=reason,
            status="TARGET_MISSING",
            message="削除対象要素が指定されていません",
        )

    if target.target not in list(target.parent):
        return XmlDeleteResult(
            deleted=False,
            label=label,
            reason=reason,
            status="TARGET_NOT_CHILD",
            message="削除対象要素が親要素の直接の子要素ではありません",
        )

    try:
        target.parent.remove(target.target)
    except Exception as exc:  # pragma: no cover - ElementTree実装差異や想定外保険
        return XmlDeleteResult(
            deleted=False,
            label=label,
            reason=reason,
            status="DELETE_ERROR",
            message=f"XML要素の削除に失敗しました: {exc}",
        )

    return XmlDeleteResult(
        deleted=True,
        label=label,
        reason=reason,
        status="DELETED",
        message="XML要素を削除しました",
    )


def xml_delete_result_to_dict(result: XmlDeleteResult) -> dict[str, Any]:
    """XmlDeleteResult をCSV出力などに使いやすい dict へ変換する。"""
    return {
        "deleted": result.deleted,
        "label": result.label,
        "reason": result.reason,
        "status": result.status,
        "message": result.message,
    }