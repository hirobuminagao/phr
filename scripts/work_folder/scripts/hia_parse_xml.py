#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hia_parse_xml.py

HIA XML parser for HIA_fund_ledger_xml pipeline.

責務:
- 1 XML から人物識別に必要な最小項目を読む
- 返却は dict とする
- 正規化や DB 記帳はここでは行わない

注意:
- XML 仕様差があり得るため、取得できない項目は None を返す
- 必須判定は呼び出し側 (hia_import_zip.py) で行う
"""

from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET


NS = {"hl7": "urn:hl7-org:v3"}


def _find_attr(root: ET.Element, xpath: str, attr_name: str) -> str | None:
    """XPath で見つかった先頭要素の属性値を返す。無ければ None。"""
    elem = root.find(xpath, NS)
    if elem is None:
        return None
    return elem.get(attr_name)



def _find_text(root: ET.Element, xpath: str) -> str | None:
    """XPath で見つかった先頭要素の text を返す。無ければ None。"""
    elem = root.find(xpath, NS)
    if elem is None or elem.text is None:
        return None
    text = elem.text.strip()
    return text or None


# 新規追加: OID指定で extension を返すヘルパ
def _find_id_extension_by_root(root: ET.Element, xpath_base: str, root_oid: str) -> str | None:
    """
    指定 base 配下の hl7:id[@root='...'] の extension を返す。
    厚労省 XML では id 要素が複数出るため、OID(root) 指定で取り分ける。
    """
    return _find_attr(root, f"{xpath_base}/hl7:id[@root='{root_oid}']", "extension")


def _build_name(root: ET.Element) -> str | None:
    """患者氏名を簡易連結して返す。"""
    name_elem = root.find(".//hl7:patientRole/hl7:patient/hl7:name", NS)
    if name_elem is None:
        return None

    parts: list[str] = []
    for child in list(name_elem):
        if child.text:
            text = child.text.strip()
            if text:
                parts.append(text)

    if not parts and name_elem.text:
        text = name_elem.text.strip()
        if text:
            parts.append(text)

    return "".join(parts) if parts else None


def _guess_name_kana(root: ET.Element) -> str | None:
    """
    氏名カナ候補を返す。

    現時点では XML ごとの差が大きいため、まずは name の text / child text をそのまま拾う。
    必要なら後で専用 XPath を追加する。
    """
    # まず患者 name をそのまま候補として返す
    return _build_name(root)


def _parse_hl7_date(value: str | None) -> str | None:
    """
    HL7 date-ish string を YYYY-MM-DD に寄せる簡易変換。
    例: 19800926 -> 1980-09-26
    """
    if not value:
        return None

    v = value.strip()
    if len(v) >= 8 and v[:8].isdigit():
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
    return None



def _parse_insurance_fields(root: ET.Element) -> tuple[str | None, str | None]:
    """
    被保険者証等の記号・番号を OID 指定 XPath で返す。

    - 記号: 1.2.392.200119.6.204
    - 番号: 1.2.392.200119.6.205
    """
    patient_role_base = ".//hl7:recordTarget/hl7:patientRole"

    insurance_symbol = _find_id_extension_by_root(
        root,
        patient_role_base,
        "1.2.392.200119.6.204",
    )
    insurance_number = _find_id_extension_by_root(
        root,
        patient_role_base,
        "1.2.392.200119.6.205",
    )
    return insurance_symbol, insurance_number


def parse_hia_xml_identity(xml_path: str | Path) -> dict:
    """
    HIA XML から人物識別に必要な最小項目を抽出する。

    Returns:
        dict with keys:
            xml_path
            exam_date
            name
            name_kana
            birthdate
            gender_code
            insurer_number
            insurance_symbol
            insurance_number
            facility_code
            facility_name
    """
    xml_path = Path(xml_path)
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # 健診日候補
    exam_date_raw = (
        _find_attr(root, ".//hl7:documentationOf//hl7:effectiveTime/hl7:low", "value")
        or _find_attr(root, ".//hl7:documentationOf//hl7:serviceEvent/hl7:effectiveTime/hl7:low", "value")
        or _find_attr(root, ".//hl7:documentationOf//hl7:serviceEvent/hl7:effectiveTime", "value")
    )
    exam_date = _parse_hl7_date(exam_date_raw)

    # 基本人情報
    patient_base = ".//hl7:recordTarget/hl7:patientRole/hl7:patient"
    name = _build_name(root)
    name_kana = _guess_name_kana(root) or name
    birthdate = _parse_hl7_date(
        _find_attr(root, f"{patient_base}/hl7:birthTime", "value")
    )
    gender_code = _find_attr(
        root,
        f"{patient_base}/hl7:administrativeGenderCode",
        "code",
    )

    # 保険者番号
    insurer_number = _find_id_extension_by_root(
        root,
        ".//hl7:recordTarget/hl7:patientRole",
        "1.2.392.200119.6.101",
    )

    insurance_symbol, insurance_number = _parse_insurance_fields(root)

    # 医療機関情報
    facility_code = (
        _find_id_extension_by_root(
            root,
            ".//hl7:author/hl7:assignedAuthor/hl7:representedOrganization",
            "1.2.392.200119.6.102",
        )
        or _find_attr(
            root,
            ".//hl7:custodian//hl7:representedCustodianOrganization/hl7:id",
            "extension",
        )
    )
    facility_name = (
        _find_text(root, ".//hl7:author/hl7:assignedAuthor/hl7:representedOrganization/hl7:name")
        or _find_text(root, ".//hl7:custodian//hl7:representedCustodianOrganization/hl7:name")
    )

    # --------------------------------------------------
    # 報告区分 / 健診プログラム
    # --------------------------------------------------

    # 報告区分
    # ClinicalDocument 直下の code[@codeSystem='1.2.392.200119.6.1001'] を採用する。
    report_category = _find_attr(
        root,
        "hl7:code[@codeSystem='1.2.392.200119.6.1001']",
        "code",
    )

    # 健診プログラムコード
    # documentationOf/serviceEvent/code[@codeSystem='1.2.392.200119.6.1002'] を採用する。
    health_program_code = _find_attr(
        root,
        ".//hl7:documentationOf/hl7:serviceEvent/hl7:code[@codeSystem='1.2.392.200119.6.1002']",
        "code",
    )

    return {
        "xml_path": str(xml_path),
        "exam_date": exam_date,
        "name": name,
        "name_kana": name_kana,
        "birthdate": birthdate,
        "gender_code": gender_code,
        "insurer_number": insurer_number,
        "insurance_symbol": insurance_symbol,
        "insurance_number": insurance_number,
        "facility_code": facility_code,
        "facility_name": facility_name,
        "report_category": report_category,
        "health_program_code": health_program_code,
    }


if __name__ == "__main__":
    # 簡易確認用:
    # python hia_parse_xml.py /path/to/file.xml
    import sys
    from pprint import pprint

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python hia_parse_xml.py <xml_path>")

    pprint(parse_hia_xml_identity(sys.argv[1]))
