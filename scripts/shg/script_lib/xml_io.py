

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xml_io.py

SHG XMLチェック用のファイル入出力ヘルパ。

責務:
- 入力ディレクトリ配下のZIP展開
- 展開済みXMLの収集
- XMLファイルの読込

非責務:
- XML内容の抽出
- identity生成
- DB接続
- outcome判定
- XML修正
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from typing import Any, Iterable
from lxml import etree as LET


XSD_RELATED_NAMES = {
    "ix08_V08.xml",
    "su08_V08.xml",
}

XSD_RELATED_DIR_NAMES = {
    "XSD",
    "CLAIMS",
}


def read_xml(xml_path: Path) -> Any:
    """XMLファイルを読み込み、root element を返す。

    lxml を使用し、コメントや既存namespace prefixをできるだけ維持する。
    戻り値は既存のXML抽出関数との型互換を優先して Any とする。
    """
    parser = LET.XMLParser(
        remove_comments=False,
        remove_pis=False,
        remove_blank_text=False,
        recover=False,
    )
    tree = LET.parse(str(xml_path), parser)
    return tree.getroot()


def is_target_xml(xml_path: Path) -> bool:
    """解析対象のXMLかどうかを判定する。

    SHGチェックでは DATA/*.xml を主対象とし、
    送付用アーカイブの管理XMLやXSD関連XMLは対象外とする。
    """
    if xml_path.name in XSD_RELATED_NAMES:
        return False

    path_parts = set(xml_path.parts)
    if path_parts & XSD_RELATED_DIR_NAMES:
        return False

    return xml_path.suffix.lower() == ".xml"


def scan_xml_paths(base_dir: Path) -> list[Path]:
    """指定ディレクトリ配下から解析対象XMLを再帰的に収集する。"""
    if not base_dir.exists():
        return []

    return sorted(
        xml_path
        for xml_path in base_dir.rglob("*.xml")
        if xml_path.is_file() and is_target_xml(xml_path)
    )


def extract_zip(zip_path: Path, work_dir: Path) -> Path:
    """ZIPをwork_dir配下へ展開し、展開先ディレクトリを返す。

    展開先は ZIPファイル名のstem を使う。
    既に同名ディレクトリが存在する場合は削除してから展開する。
    """
    extract_dir = work_dir / zip_path.stem

    if extract_dir.exists():
        shutil.rmtree(extract_dir)

    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(extract_dir)

    return extract_dir


def collect_input_xml_paths(input_dir: Path, work_dir: Path) -> tuple[list[Path], dict[Path, Path]]:
    """入力ディレクトリからXMLを収集する。

    ZIPがある場合はwork_dir配下へ展開し、その中のXMLを収集する。
    展開済みXMLがある場合も収集する。

    Returns:
        tuple[list[Path], dict[Path, Path]]:
            - 解析対象XMLパス一覧
            - XMLパス -> ZIP展開ディレクトリ の対応表

    備考:
        ZIP由来でないXMLは対応表に含めない。
    """
    xml_paths: list[Path] = []
    xml_to_extract_dir: dict[Path, Path] = {}

    if not input_dir.exists():
        return xml_paths, xml_to_extract_dir

    zip_paths = sorted(input_dir.rglob("*.zip"))
    for zip_path in zip_paths:
        extract_dir = extract_zip(zip_path, work_dir)
        extracted_xml_paths = scan_xml_paths(extract_dir)
        xml_paths.extend(extracted_xml_paths)
        for xml_path in extracted_xml_paths:
            xml_to_extract_dir[xml_path] = extract_dir

    direct_xml_paths = [
        xml_path
        for xml_path in scan_xml_paths(input_dir)
        if not any(part.endswith(".zip") for part in xml_path.parts)
    ]
    xml_paths.extend(direct_xml_paths)

    # 同じXMLが重複して入らないよう、Path文字列ベースで一意化する。
    unique_xml_paths = sorted({xml_path.resolve(): xml_path for xml_path in xml_paths}.values())

    return unique_xml_paths, xml_to_extract_dir


def remove_extract_dirs_without_fix(
    extract_dirs: Iterable[Path],
    fixed_extract_dirs: set[Path],
) -> None:
    """fixが無かったZIP展開フォルダを削除する。

    削除単位はZIP展開フォルダ単位とする。
    XML単位の部分削除は行わない。
    """
    for extract_dir in extract_dirs:
        if extract_dir in fixed_extract_dirs:
            continue
        if extract_dir.exists() and extract_dir.is_dir():
            shutil.rmtree(extract_dir)