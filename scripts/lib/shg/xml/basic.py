

from __future__ import annotations

from typing import Any
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import NS, text_or



OID_INSURER = "1.2.392.200119.6.101"
OID_SYMBOL = "1.2.392.200119.6.204"
OID_NUMBER = "1.2.392.200119.6.205"




def get_id_extension_by_oid(root: ET.Element, oid: str) -> str:
    """<id root="OID" extension="..."/> の extension を取得する。"""
    for el in root.findall(".//cda:id", NS):
        if (el.get("root") or "").strip() == oid:
            return (el.get("extension") or "").strip()
    return ""


def get_report_code(root: ET.Element) -> str:
    """文書種別系コードではなく、報告区分コードを返す。"""
    for el in root.findall(".//cda:code", NS):
        if (el.get("codeSystem") or "").strip() == "1.2.392.200119.6.1001":
            return (el.get("code") or "").strip()
    return ""


def get_name(root: ET.Element) -> str:
    return text_or(root.find(".//cda:recordTarget//cda:patient/cda:name", NS))


def get_gender(root: ET.Element) -> str:
    el = root.find(".//cda:recordTarget//cda:patient/cda:administrativeGenderCode", NS)
    if el is None:
        return ""
    return (el.get("code") or "").strip()


def get_birth(root: ET.Element) -> str:
    el = root.find(".//cda:recordTarget//cda:patient/cda:birthTime", NS)
    if el is None:
        return ""
    return (el.get("value") or "").strip()


def get_ticket_info(root: ET.Element) -> tuple[str, str]:
    """利用券（functionCode=2）の番号と有効期限を返す。"""
    ticket_no = ""
    ticket_exp = ""

    for auth in root.findall(".//cda:authorization", NS):
        code_el = auth.find(".//cda:functionCode", NS)
        if code_el is None:
            continue
        if (code_el.get("code") or "").strip() != "2":
            continue

        id_el = auth.find(".//cda:id", NS)
        if id_el is not None:
            ticket_no = (id_el.get("extension") or "").strip()

        exp_el = auth.find(".//cda:effectiveTime/cda:high", NS)
        if exp_el is not None:
            ticket_exp = (exp_el.get("value") or "").strip()
        break

    return ticket_no, ticket_exp


def extract_basic(root: ET.Element) -> dict[str, Any]:
    """SHG XML の基本情報を抽出する。"""
    insurer = get_id_extension_by_oid(root, OID_INSURER)
    symbol = get_id_extension_by_oid(root, OID_SYMBOL)
    number = get_id_extension_by_oid(root, OID_NUMBER)
    name = get_name(root)
    gender = get_gender(root)
    birth = get_birth(root)
    report_code = get_report_code(root)
    ticket_no, ticket_exp = get_ticket_info(root)

    return {
        "report_code": report_code,
        "insurer": insurer,
        "symbol": symbol,
        "number": number,
        "name": name,
        "gender": gender,
        "birth": birth,
        "ticket_no": ticket_no,
        "ticket_exp": ticket_exp,
    }