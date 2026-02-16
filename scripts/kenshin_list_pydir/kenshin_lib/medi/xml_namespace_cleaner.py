

# -*- coding: utf-8 -*-
"""
xml_namespace_cleaner.py

【固定化（freeze）】
HIAアップロード用XMLの namespace/prefix 問題を確実に除去する専用ユーティリティ。

対象（今回確定仕様）
・<ns0:XXX> → <XXX>
・</ns0:XXX> → </XXX>
・xmlns:ns0="..." 削除
・default namespace を除去

ポイント
文字列置換ではなく XML再シリアライズで実施する。
= 正しいXMLとして再生成するため壊れない。
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path


# -----------------------------
# core
# -----------------------------

def _strip_namespace(tag: str) -> str:
    """{namespace}tag → tag に変換"""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag


def _remove_namespaces(elem: ET.Element) -> None:
    """ツリー全体からnamespaceを除去"""
    elem.tag = _strip_namespace(elem.tag)
    for child in list(elem):
        _remove_namespaces(child)


# -----------------------------
# public API
# -----------------------------

def clean_xml_namespaces(xml_text: str) -> str:
    """
    XML文字列 → namespace除去 → 再シリアライズ文字列
    """
    root = ET.fromstring(xml_text)
    _remove_namespaces(root)
    return ET.tostring(root, encoding="unicode", short_empty_elements=False)


def clean_xml_file(path: Path) -> None:
    """
    XMLファイルを直接上書きでnamespace除去
    """
    txt = path.read_text(encoding="utf-8")
    cleaned = clean_xml_namespaces(txt)
    if not cleaned.endswith("\n"):
        cleaned += "\n"
    path.write_text(cleaned, encoding="utf-8")


def clean_xml_directory(dir_path: Path) -> None:
    """
    フォルダ内の *.xml を一括クリーン
    """
    for fp in dir_path.rglob("*.xml"):
        clean_xml_file(fp)