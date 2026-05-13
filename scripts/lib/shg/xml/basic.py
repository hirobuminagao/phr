from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple
import xml.etree.ElementTree as ET

from scripts.lib.shg.xml.common import NS, text_or



OID_INSURER = "1.2.392.200119.6.101"
OID_SYMBOL = "1.2.392.200119.6.204"
OID_NUMBER = "1.2.392.200119.6.205"


@dataclass(frozen=True)
class XmlValueLocation:
    """XML上の値位置。

    elem の attr_name 属性が、対象値の格納位置である。
    """

    elem: ET.Element
    attr_name: str

    def current_value(self) -> str:
        """現在値を文字列で返す。"""
        return (self.elem.get(self.attr_name) or "").strip()


@dataclass(frozen=True)
class TicketInfo:
    """利用券情報の値とXML上の値位置。"""

    ticket_no: Optional[str]
    ticket_exp: Optional[str]
    ticket_no_location: Optional[XmlValueLocation]
    ticket_exp_location: Optional[XmlValueLocation]


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


def get_ticket_info_detail(root: ET.Element) -> TicketInfo:
    """利用券(functionCode=2)の整理番号・有効期限とXML上の値位置を取得する。

    注意:
    - 利用券 participant は functionCode/@code = "2" で識別する
    - participant の出現順による推定は禁止する
    - XML更新は行わず、値と位置情報のみを返す
    """
    for participant in root.findall(".//cda:participant", NS):
        func_el = participant.find("cda:functionCode", NS)
        if func_el is None:
            continue
        if (func_el.get("code") or "").strip() != "2":
            continue

        ticket_no: Optional[str] = None
        ticket_exp: Optional[str] = None
        ticket_no_location: Optional[XmlValueLocation] = None
        ticket_exp_location: Optional[XmlValueLocation] = None

        id_el = participant.find("cda:associatedEntity/cda:id", NS)
        if id_el is not None:
            ticket_no_location = XmlValueLocation(elem=id_el, attr_name="extension")
            ticket_no = ticket_no_location.current_value() or None

        high_el = participant.find("cda:time/cda:high", NS)
        if high_el is not None:
            ticket_exp_location = XmlValueLocation(elem=high_el, attr_name="value")
            ticket_exp = ticket_exp_location.current_value() or None

        return TicketInfo(
            ticket_no=ticket_no,
            ticket_exp=ticket_exp,
            ticket_no_location=ticket_no_location,
            ticket_exp_location=ticket_exp_location,
        )

    return TicketInfo(
        ticket_no=None,
        ticket_exp=None,
        ticket_no_location=None,
        ticket_exp_location=None,
    )


def get_ticket_info(root: ET.Element) -> TicketInfo:
    """利用券(functionCode=2)の整理番号・有効期限とXML上の値位置を取得する。"""
    return get_ticket_info_detail(root)


def get_ticket_values(root: ET.Element) -> Tuple[Optional[str], Optional[str]]:
    """既存互換用の利用券値取得関数。

    tuple形式を必要とする旧コード向け。
    新規コードでは get_ticket_info を使用する。
    """
    detail = get_ticket_info_detail(root)
    return detail.ticket_no, detail.ticket_exp


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
    ticket_info = get_ticket_info(root)

    return {
        "report_code": report_code,
        "final_date": final_date,
        "insurer": insurer,
        "symbol": symbol,
        "number": number,
        "name": name,
        "gender": gender,
        "birth": birth,
        "ticket_no": ticket_info.ticket_no,
        "ticket_exp": ticket_info.ticket_exp,
    }