from __future__ import annotations

from typing import Any, Optional, Tuple
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import NS, text_or



OID_INSURER = "1.2.392.200119.6.101"
OID_SYMBOL = "1.2.392.200119.6.204"
OID_NUMBER = "1.2.392.200119.6.205"




def get_id_extension_by_oid(root: ET.Element, oid: str) -> Optional[str]:
    """<id root="OID" extension="..."/> の extension を取得する。"""
    for el in root.findall(".//cda:id", NS):
        if (el.get("root") or "").strip() == oid:
            return (el.get("extension") or "").strip()
    return None


def get_report_code(root: ET.Element) -> Optional[str]:
    """文書種別系コードではなく、報告区分コードを返す。"""
    for el in root.findall(".//cda:code", NS):
        if (el.get("codeSystem") or "").strip() == "1.2.392.200119.6.1001":
            return (el.get("code") or "").strip()
    return None


def get_final_date(root: ET.Element, report_code: Optional[str]) -> Optional[str]:
    """report_code=22 の場合のみ、documentationOf/serviceEvent/effectiveTime から final_date を取得する。"""
    if (report_code or "").strip() != "22":
        return None

    el = root.find("cda:documentationOf/cda:serviceEvent/cda:effectiveTime", NS)
    if el is None:
        return None

    val = (el.get("value") or "").strip()
    return val if val else None


def get_name(root: ET.Element) -> Optional[str]:
    val = text_or(root.find(".//cda:recordTarget//cda:patient/cda:name", NS))
    return val if val else None


def get_gender(root: ET.Element) -> Optional[str]:
    el = root.find(".//cda:recordTarget//cda:patient/cda:administrativeGenderCode", NS)
    if el is None:
        return None
    val = (el.get("code") or "").strip()
    return val if val else None


def get_birth(root: ET.Element) -> Optional[str]:
    el = root.find(".//cda:recordTarget//cda:patient/cda:birthTime", NS)
    if el is None:
        return None
    val = (el.get("value") or "").strip()
    return val if val else None


def get_ticket_info(root: ET.Element) -> Tuple[Optional[str], Optional[str]]:
    """利用券（functionCode=2）の番号と有効期限を返す。

    SHG XML では実装差異がありうるため、authorization を起点に複数パターンを許容する。
    """
    ticket_no = None
    ticket_exp = None

    for auth in root.findall(".//cda:authorization", NS):
        code_el = auth.find(".//cda:functionCode", NS)
        if code_el is None:
            continue
        if (code_el.get("code") or "").strip() != "2":
            continue

        # 1) もっとも素直な id / high
        id_el = auth.find(".//cda:id", NS)
        if id_el is not None:
            ticket_no = (id_el.get("extension") or "").strip()

        exp_el = auth.find(".//cda:effectiveTime/cda:high", NS)
        if exp_el is not None:
            ticket_exp = (exp_el.get("value") or "").strip()

        # 2) 代替パターン: descendant を広めに走査
        if ticket_no is None:
            for cand in auth.findall(".//cda:id", NS):
                ext = (cand.get("extension") or "").strip()
                if ext:
                    ticket_no = ext
                    break

        if ticket_exp is None:
            for cand in auth.findall(".//cda:high", NS):
                val = (cand.get("value") or "").strip()
                if val:
                    ticket_exp = val
                    break

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
    final_date = get_final_date(root, report_code)
    ticket_no, ticket_exp = get_ticket_info(root)

    return {
        "report_code": report_code,
        "final_date": final_date,
        "insurer": insurer,
        "symbol": symbol,
        "number": number,
        "name": name,
        "gender": gender,
        "birth": birth,
        "ticket_no": ticket_no,
        "ticket_exp": ticket_exp,
    }