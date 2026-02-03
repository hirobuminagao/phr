# -*- coding: utf-8 -*-
"""
scripts/medi_export_xml.py

【目的】
work_other.medi_exam_result_ledger / work_other.medi_exam_result_item_values を元に、
dev_phr.exam_item_master を参照しつつ、厚労省標準様式（CDA）個人XML＋IX08 を生成し、
「医療機関 × 提出先健保」ごとに 1 ZIP を作る（厚労省フォーマットをデフォルト固定）。

【前提フォルダ】
KENSHIN_LIST_PYDIR/
  scripts/
    medi_export_xml.py    (このファイル)
  medi_export_xml/        (出力先 root)
  xsd/                    (同梱するXSD。フォルダごとコピー)

【ENV】（DB以外）
  EXPORT_ROOT
  EXPORT_FILE_DATE (YYYYMMDD)
  EXPORT_LIMIT
  EXPORT_IX08_NAME
  EXPORT_XML_ENCODING

  # 厚労省：同日分割送信回数(N) と 実施区分コード(X)
  EXPORT_SPLIT_NO=0
  EXPORT_IMPL_CODE=1

  # 厚労省：個人XMLファイル名 種別（表2の「種別」= 1桁）
  # 例：特定健診データファイル = 1（運用に合わせて変更）
  EXPORT_FILE_KIND=1

  # ルートフォルダ名テンプレ（空なら厚労省規則で自動生成）
  # 例：{sender}_{receiver}_{date}{split}_{impl}
  EXPORT_GROUP_DIR_TEMPLATE=

  # ZIP名テンプレ（空なら「{root_dir}.zip」）
  EXPORT_ZIP_NAME_TEMPLATE=
"""

from __future__ import annotations

import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import xml.etree.ElementTree as ET

from dotenv import load_dotenv

import mysql.connector
from mysql.connector.cursor import MySQLCursorDict


# --- path bootstrap（normalize_item_values.py と同じ） ---
BASE_DIR = Path(__file__).resolve().parents[1]  # = kenshin_list_pydir
load_dotenv(BASE_DIR / ".env")
# --------------------------------------------------------


# -----------------------------
# Namespaces
# -----------------------------
NS_HL7 = "urn:hl7-org:v3"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_MHLW_INDEX = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/0000161103.html"

# CDA は default namespace（xmlns="urn:hl7-org:v3"）で出したい
ET.register_namespace("", NS_HL7)
ET.register_namespace("xsi", NS_XSI)
# Index は別NSなので prefix を付けて出す（default を奪わない）
ET.register_namespace("ix", NS_MHLW_INDEX)


# -----------------------------
# env utils
# -----------------------------
def env_required(key: str) -> str:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        raise RuntimeError(f"必須環境変数 {key} が設定されていません")
    return v.strip()


def env_optional(key: str, default: str = "") -> str:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip()


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def parse_env_file_date() -> str:
    v = env_optional("EXPORT_FILE_DATE", "")
    if v:
        if not re.match(r"^\d{8}$", v):
            raise ValueError("EXPORT_FILE_DATE must be YYYYMMDD (8 digits).")
        return v
    return yyyymmdd(datetime.now())


def ensure_tel_prefix(t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    s = str(t).strip()
    if not s:
        return None
    return s if s.startswith("tel:") else f"tel:{s}"


def safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(str(x))
        except Exception:
            return None


def fmt_date_yyyymmdd(d: Any) -> Optional[str]:
    if d is None:
        return None
    try:
        import datetime as pydt

        if isinstance(d, pydt.datetime):
            return d.strftime("%Y%m%d")
        if isinstance(d, pydt.date):
            return d.strftime("%Y%m%d")
    except Exception:
        pass

    s = str(d).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})-?(\d{2})-?(\d{2})", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    return None


def zero_pad_numeric(s: Optional[str], width: int) -> Optional[str]:
    """数字だけならゼロ埋め。数字以外（英数混在など）はそのまま。"""
    if not s:
        return None
    t = s.strip()
    if not t:
        return None
    return t.zfill(width) if t.isdigit() else t


# -----------------------------
# XML pretty print（Pylance対策込み）
# -----------------------------
def indent_xml(elem: ET.Element, level: int = 0, space: str = "  ") -> None:
    """
    ElementTree を人間が読めるようにインデントする。
    Python 3.9+ は ET.indent を優先、無い場合は互換実装。
    """
    if hasattr(ET, "indent"):
        # type: ignore[attr-defined]
        ET.indent(elem, space=space, level=level)
        return

    i = "\n" + level * space
    if len(elem):
        if not (elem.text and elem.text.strip()):
            elem.text = i + space
        for e in elem:
            indent_xml(e, level + 1, space)
        if not (elem.tail and elem.tail.strip()):
            elem.tail = i
    else:
        if level and not (elem.tail and elem.tail.strip()):
            elem.tail = i


# -----------------------------
# コメントを追加
# -----------------------------
def add_comment(parent: ET.Element, text: str) -> None:
    """
    XMLコメントを追加する。スキーマ上は無視されるので可読性目的で使える。
    """
    parent.append(ET.Comment(f" {text} "))


# -----------------------------
# DB connect (mysql.connector固定)
# -----------------------------
def load_work_other_params() -> dict:
    host = env_required("MEDI_IMPORT_DB_HOST")
    port = int(env_required("MEDI_IMPORT_DB_PORT"))
    name = env_optional("MEDI_IMPORT_DB_NAME", "work_other") or "work_other"
    user = env_required("MEDI_IMPORT_DB_USER")
    password = env_required("MEDI_IMPORT_DB_PASSWORD")
    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": True,
        "use_pure": True,
    }


def connect_mysql(params: dict):
    return mysql.connector.connect(**params)


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


# -----------------------------
# Data models
# -----------------------------
@dataclass
class LedgerRow:
    ledger_id: int
    health_examination_date: Optional[str]  # YYYYMMDD
    insurer_number: Optional[str]           # 8桁想定（ゼロ埋め）

    insurance_card_symbol: Optional[str]
    insurance_card_number: Optional[str]
    name_full: Optional[str]
    name_kana: Optional[str]
    gender_code: Optional[str]
    birthday: Optional[str]                 # YYYYMMDD
    health_exam_report_category: Optional[str]
    program_code: Optional[str]
    postalcode: Optional[str]
    address: Optional[str]

    org_name: Optional[str]
    org_no: Optional[str]                   # 医療機関番号（10桁想定・ゼロ埋め）
    org_postalcode: Optional[str]
    org_address: Optional[str]
    org_tel: Optional[str]

    symbol_match: Optional[str]
    number_match: Optional[str]
    name_kana_match: Optional[str]


@dataclass
class ItemRow:
    namecode: str
    item_name: Optional[str]
    xml_value_type: Optional[str]  # PQ/CD/CO/ST
    result_code_oid: Optional[str]
    display_unit: Optional[str]
    ucum_unit: Optional[str]
    xml_method_code: Optional[str]
    jun_no: Optional[int]
    value: Optional[str]
    nullflavor: Optional[str]
    value_seq: int


# -----------------------------
# SQL
# -----------------------------
LEDGER_SQL = """
SELECT
  ledger_id,
  health_examination_date,
  insurer_number,
  insurance_card_symbol,
  insurance_card_number,
  name_full,
  name_kana,
  gender_code,
  birthday,
  health_exam_report_category,
  program_code,
  postalcode,
  address,
  health_examination_organization_name,
  health_examination_organization_no,
  health_examination_organization_postalcode,
  health_examination_organization_address,
  health_examination_organization_tel,
  insurance_card_symbol_match,
  insurance_card_number_match,
  name_kana_match
FROM work_other.medi_exam_result_ledger
ORDER BY health_examination_organization_no, insurer_number, ledger_id
"""

ITEMS_SQL = """
SELECT
  iv.namecode,
  m.item_name,
  m.xml_value_type,
  m.result_code_oid,
  m.display_unit,
  m.ucum_unit,
  m.xml_method_code,
  m.jun_no,
  iv.value,
  iv.nullflavor,
  iv.value_seq
FROM work_other.medi_exam_result_item_values iv
JOIN dev_phr.exam_item_master m
  ON m.namecode = iv.namecode
WHERE iv.ledger_id = %s
ORDER BY
  CASE WHEN m.jun_no IS NULL THEN 1 ELSE 0 END,
  m.jun_no,
  iv.namecode,
  iv.value_seq
"""


# -----------------------------
# XML builders (HL7 CDA)
# -----------------------------
def build_clinical_document_xml(
    ledger: LedgerRow,
    items: List[ItemRow],
    file_date: str,
) -> ET.ElementTree:
    root = ET.Element(
        f"{{{NS_HL7}}}ClinicalDocument",
        {f"{{{NS_XSI}}}schemaLocation": f"{NS_HL7} ../XSD/hc08_V08.xsd"},
    )

    # --- header ---
    add_comment(root, "CDA typeId")
    type_id = ET.SubElement(root, f"{{{NS_HL7}}}typeId")
    type_id.set("extension", "POCD_HD000040")
    type_id.set("root", "2.16.840.1.113883.1.3")

    # clinicalDocument/id は「NI」でOK（ここは議論済みの決定事項）
    add_comment(root, "ClinicalDocument ID（決定事項：nullFlavor=NI）")
    ET.SubElement(root, f"{{{NS_HL7}}}id").set("nullFlavor", "NI")

    add_comment(root, "報告区分")
    report_code = safe_text(ledger.health_exam_report_category) or "10"
    code = ET.SubElement(root, f"{{{NS_HL7}}}code")
    code.set("code", report_code)
    code.set("codeSystem", "1.2.392.200119.6.1001")

    add_comment(root, "ファイル作成日")
    ET.SubElement(root, f"{{{NS_HL7}}}effectiveTime").set("value", file_date)

    add_comment(root, "機密区分")
    confidentiality_code = ET.SubElement(root, f"{{{NS_HL7}}}confidentialityCode")
    confidentiality_code.set("code", "N")
    confidentiality_code.set("codeSystem", "2.16.840.1.113883.5.25")

    # --- 受診者情報 ---
    add_comment(root, "受診者情報")
    record_target = ET.SubElement(root, f"{{{NS_HL7}}}recordTarget")
    patient_role = ET.SubElement(record_target, f"{{{NS_HL7}}}patientRole")

    # ids
    add_comment(patient_role, "保険者番号")
    insurer_ext = safe_text(ledger.insurer_number) or ""
    ET.SubElement(
        patient_role,
        f"{{{NS_HL7}}}id",
        {"extension": insurer_ext, "root": "1.2.392.200119.6.101"},
    )

    add_comment(patient_role, "被保険者証等記号")
    sym = safe_text(ledger.symbol_match) or safe_text(ledger.insurance_card_symbol) or ""
    ET.SubElement(
        patient_role,
        f"{{{NS_HL7}}}id",
        {"extension": sym, "root": "1.2.392.200119.6.204"},
    )

    add_comment(patient_role, "被保険者証等番号")
    num = safe_text(ledger.number_match) or safe_text(ledger.insurance_card_number) or ""
    ET.SubElement(
        patient_role,
        f"{{{NS_HL7}}}id",
        {"extension": num, "root": "1.2.392.200119.6.205"},
    )

    # addr/postalCode（郵便番号のみ）
    if safe_text(ledger.postalcode):
        add_comment(patient_role, "住所と郵便番号")
        addr = ET.SubElement(patient_role, f"{{{NS_HL7}}}addr")
        ET.SubElement(addr, f"{{{NS_HL7}}}postalCode").text = safe_text(ledger.postalcode)

    # patient
    patient = ET.SubElement(patient_role, f"{{{NS_HL7}}}patient")

    add_comment(patient, "氏名")
    nm = safe_text(ledger.name_kana_match) or safe_text(ledger.name_kana) or safe_text(ledger.name_full) or ""
    ET.SubElement(patient, f"{{{NS_HL7}}}name").text = nm

    add_comment(patient, "男女区分")
    g = safe_text(ledger.gender_code) or "9"
    ET.SubElement(
        patient,
        f"{{{NS_HL7}}}administrativeGenderCode",
        {"code": g, "codeSystem": "1.2.392.200119.6.1104"},
    )

    add_comment(patient, "生年月日")
    ET.SubElement(
        patient,
        f"{{{NS_HL7}}}birthTime",
        {"value": safe_text(ledger.birthday) or ""},
    )

    # --- author（作成機関情報）---
    add_comment(root, "特定健診情報ファイル作成機関情報")
    author = ET.SubElement(root, f"{{{NS_HL7}}}author")

    add_comment(author, "ファイル作成日（author/time）")
    ET.SubElement(author, f"{{{NS_HL7}}}time", {"value": file_date})

    assigned_author = ET.SubElement(author, f"{{{NS_HL7}}}assignedAuthor")
    ET.SubElement(assigned_author, f"{{{NS_HL7}}}id", {"nullFlavor": "NI"})

    rep_org = ET.SubElement(assigned_author, f"{{{NS_HL7}}}representedOrganization")

    add_comment(rep_org, "特定健診機関番号")
    ET.SubElement(
        rep_org,
        f"{{{NS_HL7}}}id",
        {"extension": safe_text(ledger.org_no) or "", "root": "1.2.392.200119.6.102"},
    )

    if safe_text(ledger.org_name):
        add_comment(rep_org, "名称")
        ET.SubElement(rep_org, f"{{{NS_HL7}}}name").text = safe_text(ledger.org_name)

    telv = ensure_tel_prefix(ledger.org_tel)
    if telv:
        add_comment(rep_org, "電話番号")
        ET.SubElement(rep_org, f"{{{NS_HL7}}}telecom", {"value": telv})

    if safe_text(ledger.org_postalcode):
        add_comment(rep_org, "所在地と郵便番号")
        oaddr = ET.SubElement(rep_org, f"{{{NS_HL7}}}addr")
        ET.SubElement(oaddr, f"{{{NS_HL7}}}postalCode").text = safe_text(ledger.org_postalcode)

    # --- custodian（NI固定）---
    add_comment(root, "custodian（管理組織：NI固定）")
    custodian = ET.SubElement(root, f"{{{NS_HL7}}}custodian")
    assigned_custodian = ET.SubElement(custodian, f"{{{NS_HL7}}}assignedCustodian")
    rep_cust_org = ET.SubElement(
        assigned_custodian, f"{{{NS_HL7}}}representedCustodianOrganization"
    )
    ET.SubElement(rep_cust_org, f"{{{NS_HL7}}}id", {"nullFlavor": "NI"})

    # --- documentationOf/serviceEvent（健診実施情報）---
    add_comment(root, "健診実施情報（documentationOf/serviceEvent）")
    doc_of = ET.SubElement(root, f"{{{NS_HL7}}}documentationOf")
    service_event = ET.SubElement(doc_of, f"{{{NS_HL7}}}serviceEvent")

    add_comment(service_event, "健診実施時のプログラム種別")
    program = safe_text(ledger.program_code) or "010"
    ET.SubElement(
        service_event,
        f"{{{NS_HL7}}}code",
        {"code": program, "codeSystem": "1.2.392.200119.6.1002"},
    )

    add_comment(service_event, "健診実施年月日")
    he_date = safe_text(ledger.health_examination_date) or ""
    ET.SubElement(service_event, f"{{{NS_HL7}}}effectiveTime", {"value": he_date})

    performer = ET.SubElement(service_event, f"{{{NS_HL7}}}performer", {"typeCode": "PRF"})
    assigned_entity = ET.SubElement(performer, f"{{{NS_HL7}}}assignedEntity")
    ET.SubElement(assigned_entity, f"{{{NS_HL7}}}id", {"nullFlavor": "NI"})

    rep_org2 = ET.SubElement(assigned_entity, f"{{{NS_HL7}}}representedOrganization")

    add_comment(rep_org2, "健診実施機関番号")
    ET.SubElement(
        rep_org2,
        f"{{{NS_HL7}}}id",
        {"extension": safe_text(ledger.org_no) or "", "root": "1.2.392.200119.6.102"},
    )

    if safe_text(ledger.org_name):
        add_comment(rep_org2, "健診実施機関名称")
        ET.SubElement(rep_org2, f"{{{NS_HL7}}}name").text = safe_text(ledger.org_name)

    telv2 = ensure_tel_prefix(ledger.org_tel)
    if telv2:
        add_comment(rep_org2, "健診実施機関電話番号")
        ET.SubElement(rep_org2, f"{{{NS_HL7}}}telecom", {"value": telv2})

    if safe_text(ledger.org_postalcode):
        add_comment(rep_org2, "健診実施機関所在地と郵便番号")
        oaddr2 = ET.SubElement(rep_org2, f"{{{NS_HL7}}}addr")
        ET.SubElement(oaddr2, f"{{{NS_HL7}}}postalCode").text = safe_text(ledger.org_postalcode)

    # --- body ---
    add_comment(root, "健診結果情報（component/structuredBody）")
    component = ET.SubElement(root, f"{{{NS_HL7}}}component")
    structured_body = ET.SubElement(component, f"{{{NS_HL7}}}structuredBody")
    body_comp = ET.SubElement(structured_body, f"{{{NS_HL7}}}component")
    section = ET.SubElement(body_comp, f"{{{NS_HL7}}}section")

    add_comment(section, "CDAセクションのコード（検査・問診結果）")
    ET.SubElement(
        section,
        f"{{{NS_HL7}}}code",
        {"code": "01010", "codeSystem": "1.2.392.200119.6.1010", "displayName": "検査・問診結果セクション"},
    )

    add_comment(section, "セクションタイトル")
    ET.SubElement(section, f"{{{NS_HL7}}}title").text = "検査・問診結果セクション"

    add_comment(section, "セクション本文（text）")
    ET.SubElement(section, f"{{{NS_HL7}}}text")

    # entries（ここはコメント入れない：ユーザー指定）
    for it in items:
        entry = ET.SubElement(section, f"{{{NS_HL7}}}entry")
        obs = ET.SubElement(entry, f"{{{NS_HL7}}}observation", {"classCode": "OBS", "moodCode": "EVN"})

        c = ET.SubElement(obs, f"{{{NS_HL7}}}code", {"code": it.namecode})
        if safe_text(it.item_name):
            c.set("displayName", safe_text(it.item_name) or "")

        vtype = safe_text(it.xml_value_type) or "ST"
        val = ET.SubElement(obs, f"{{{NS_HL7}}}value")
        val.set(f"{{{NS_XSI}}}type", vtype)

        # nullFlavor優先
        if safe_text(it.nullflavor):
            val.set("nullFlavor", safe_text(it.nullflavor) or "")
        else:
            if vtype == "PQ":
                vv = safe_text(it.value)
                if vv is None:
                    val.set("nullFlavor", "NI")
                else:
                    val.set("value", vv)
                    unit = safe_text(it.ucum_unit) or safe_text(it.display_unit)
                    if unit:
                        val.set("unit", unit)

            elif vtype in ("CD", "CO"):
                cv = safe_text(it.value)
                if cv is None:
                    val.set("nullFlavor", "NI")
                else:
                    val.set("code", cv)
                    oid = safe_text(it.result_code_oid)
                    if oid:
                        val.set("codeSystem", oid)

            else:
                sv = safe_text(it.value)
                if sv is None:
                    val.set("nullFlavor", "NI")
                else:
                    val.text = sv

        mc = safe_text(it.xml_method_code)
        if mc:
            ET.SubElement(obs, f"{{{NS_HL7}}}methodCode", {"code": mc})

    return ET.ElementTree(root)


# -----------------------------
# IX08 builder (V08)
# -----------------------------
def build_ix08_xml(
    sender_extension: str,               # 医療機関番号（10桁想定）
    file_date: str,
    total_count: int,
    receiver_extension: Optional[str],   # 提出先健保（保険者番号8桁想定）
) -> ET.ElementTree:
    root = ET.Element(
        f"{{{NS_MHLW_INDEX}}}index",
        {f"{{{NS_XSI}}}schemaLocation": f"{NS_MHLW_INDEX} ./XSD/ix08_V08.xsd"},
    )

    add_comment(root, "interactionType（固定：6）")
    ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}interactionType", {"code": "6"})

    add_comment(root, "creationTime（ファイル作成日）")
    ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}creationTime", {"value": file_date})

    add_comment(root, "sender（提出元：医療機関番号）")
    sender = ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}sender")
    ET.SubElement(
        sender,
        f"{{{NS_MHLW_INDEX}}}id",
        {"root": "1.2.392.200119.6.102", "extension": sender_extension},
    )

    if receiver_extension and receiver_extension != "UNKNOWN_INSURER":
        add_comment(root, "receiver（提出先：保険者番号）")
        receiver = ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}receiver")
        ET.SubElement(
            receiver,
            f"{{{NS_MHLW_INDEX}}}id",
            {"root": "1.2.392.200119.6.101", "extension": receiver_extension},
        )

    add_comment(root, "serviceEventType（固定：1）")
    ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}serviceEventType", {"code": "1"})

    add_comment(root, "totalRecordCount（個人XML件数）")
    ET.SubElement(root, f"{{{NS_MHLW_INDEX}}}totalRecordCount", {"value": str(total_count)})

    return ET.ElementTree(root)


# -----------------------------
# IO
# -----------------------------
def write_xml(tree: ET.ElementTree, path: Path, encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # tree.getroot() は通常 None にならない。型スタブ差で怒られる環境向けに cast して明示。
    root = cast(ET.Element, tree.getroot())
    indent_xml(root)

    tree.write(path, encoding=encoding, xml_declaration=True)


def copy_xsd_folder(project_root: Path, dst_xsd_dir: Path) -> None:
    src = project_root / "xsd"
    if not src.exists():
        raise FileNotFoundError(f"XSD folder not found: {src}")
    if dst_xsd_dir.exists():
        shutil.rmtree(dst_xsd_dir)
    shutil.copytree(src, dst_xsd_dir)


def make_zip_from_export_root(zip_path: Path, export_root: Path, include_root_dir_name: str) -> None:
    """
    export_root 直下の「ルートフォルダ」を丸ごとZIPする。
    ZIP内部のトップは include_root_dir_name/ になる（厚労省配布想定に合わせる）。
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    base_dir = export_root
    root_dir = export_root / include_root_dir_name
    if not root_dir.is_dir():
        raise FileNotFoundError(f"Root dir not found for zip: {root_dir}")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fp in root_dir.rglob("*"):
            if fp.is_file():
                arc = fp.relative_to(base_dir).as_posix()  # => {root_dir_name}/DATA/...
                zf.write(fp, arc)


# -----------------------------
# Fetchers
# -----------------------------
def fetch_ledgers(conn) -> List[LedgerRow]:
    cur = dict_cursor(conn)
    cur.execute(LEDGER_SQL)
    rows_any = cur.fetchall() or []
    cur.close()

    out: List[LedgerRow] = []
    for r0 in rows_any:
        if not isinstance(r0, dict):
            continue
        r = cast(Dict[str, Any], r0)

        out.append(
            LedgerRow(
                ledger_id=int(safe_int(r.get("ledger_id")) or 0),
                health_examination_date=fmt_date_yyyymmdd(r.get("health_examination_date")),
                insurer_number=zero_pad_numeric(safe_text(r.get("insurer_number")), 8),

                insurance_card_symbol=safe_text(r.get("insurance_card_symbol")),
                insurance_card_number=safe_text(r.get("insurance_card_number")),
                name_full=safe_text(r.get("name_full")),
                name_kana=safe_text(r.get("name_kana")),
                gender_code=safe_text(r.get("gender_code")),
                birthday=fmt_date_yyyymmdd(r.get("birthday")),
                health_exam_report_category=safe_text(r.get("health_exam_report_category")),
                program_code=safe_text(r.get("program_code")),
                postalcode=safe_text(r.get("postalcode")),
                address=safe_text(r.get("address")),

                org_name=safe_text(r.get("health_examination_organization_name")),
                org_no=zero_pad_numeric(safe_text(r.get("health_examination_organization_no")), 10),
                org_postalcode=safe_text(r.get("health_examination_organization_postalcode")),
                org_address=safe_text(r.get("health_examination_organization_address")),
                org_tel=safe_text(r.get("health_examination_organization_tel")),

                symbol_match=safe_text(r.get("insurance_card_symbol_match")),
                number_match=safe_text(r.get("insurance_card_number_match")),
                name_kana_match=safe_text(r.get("name_kana_match")),
            )
        )
    return out


def fetch_items_for_ledger(conn, ledger_id: int) -> List[ItemRow]:
    cur = dict_cursor(conn)
    cur.execute(ITEMS_SQL, (ledger_id,))
    rows_any = cur.fetchall() or []
    cur.close()

    out: List[ItemRow] = []
    for r0 in rows_any:
        if not isinstance(r0, dict):
            continue
        r = cast(Dict[str, Any], r0)

        out.append(
            ItemRow(
                namecode=str(r.get("namecode") or ""),
                item_name=safe_text(r.get("item_name")),
                xml_value_type=safe_text(r.get("xml_value_type")),
                result_code_oid=safe_text(r.get("result_code_oid")),
                display_unit=safe_text(r.get("display_unit")),
                ucum_unit=safe_text(r.get("ucum_unit")),
                xml_method_code=safe_text(r.get("xml_method_code")),
                jun_no=safe_int(r.get("jun_no")),
                value=safe_text(r.get("value")),
                nullflavor=safe_text(r.get("nullflavor")),
                value_seq=int(safe_int(r.get("value_seq")) or 1),
            )
        )
    return out


# -----------------------------
# Naming (厚労省固定)
# -----------------------------
def get_split_no() -> str:
    # 0-9（未設定は 0）
    s = env_optional("EXPORT_SPLIT_NO", "0") or "0"
    s = s.strip()
    if not re.match(r"^[0-9]$", s):
        return "0"
    return s


def get_impl_code() -> str:
    # 実施区分コード（未設定は 1）
    x = env_optional("EXPORT_IMPL_CODE", "1") or "1"
    x = x.strip()
    # 厳密なバリデーションは後でOK、いったん空は1に寄せる
    return x if x else "1"


def get_file_kind() -> str:
    # 種別（表2） 1桁（未設定は 1）
    k = env_optional("EXPORT_FILE_KIND", "1") or "1"
    k = k.strip()
    if not re.match(r"^[0-9]$", k):
        return "1"
    return k


def mhlw_root_dirname(sender_no_10: str, receiver_no_8: str, file_date: str) -> str:
    # [提出元機関番号]_[提出先機関番号]_[YYYYMMDD][N]_[X]
    n = get_split_no()
    x = get_impl_code()
    return f"{sender_no_10}_{receiver_no_8}_{file_date}{n}_{x}"


def group_dirname(sender_no_10: str, receiver_no_8: str, file_date: str) -> str:
    tmpl = env_optional("EXPORT_GROUP_DIR_TEMPLATE", "") or ""
    if tmpl.strip():
        # 必要なら上書きできるが、デフォルトは厚労省固定
        return tmpl.format(sender=sender_no_10, receiver=receiver_no_8, date=file_date, split=get_split_no(), impl=get_impl_code())
    return mhlw_root_dirname(sender_no_10, receiver_no_8, file_date)


def person_xml_filename(sender_no_10: str, file_date: str, seq6: int) -> str:
    """
    厚労省：個人XMLファイル名
    h + 健診機関番号(10桁) + yyyymmdd + N + 種別(1桁) + 6桁任意番号 + .xml
    例: h01234567892024060501000001.xml
    """
    n = get_split_no()
    kind = get_file_kind()
    return f"h{sender_no_10}{file_date}{n}{kind}{seq6:06d}.xml"


def ix08_filename() -> str:
    return env_optional("EXPORT_IX08_NAME", "ix08.xml") or "ix08.xml"


def zip_filename(root_dir_name: str) -> str:
    tmpl = env_optional("EXPORT_ZIP_NAME_TEMPLATE", "") or ""
    if tmpl.strip():
        return tmpl.format(root_dir=root_dir_name)
    # デフォルト：ルートフォルダ名.zip（厚労省寄せ）
    return f"{root_dir_name}.zip"


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    file_date = parse_env_file_date()
    encoding = env_optional("EXPORT_XML_ENCODING", "utf-8") or "utf-8"

    export_root = env_optional("EXPORT_ROOT", "")
    if not export_root:
        export_root = str(BASE_DIR / "medi_export_xml")
    export_root_path = Path(export_root)
    export_root_path.mkdir(parents=True, exist_ok=True)

    export_limit_raw = env_optional("EXPORT_LIMIT", "")
    export_limit: Optional[int] = int(export_limit_raw) if export_limit_raw else None

    conn = connect_mysql(load_work_other_params())
    try:
        ledgers = fetch_ledgers(conn)
        if export_limit is not None:
            ledgers = ledgers[:export_limit]

        # group by (sender_org_no(10), receiver_insurer_no(8))
        groups: Dict[Tuple[str, str], List[LedgerRow]] = {}
        for l in ledgers:
            sender_no = l.org_no or "0000000000"
            receiver_no = l.insurer_number or "00000000"
            # 念のため固定桁に寄せる（数字ならゼロ埋め）
            sender_no_10 = zero_pad_numeric(sender_no, 10) or sender_no
            receiver_no_8 = zero_pad_numeric(receiver_no, 8) or receiver_no

            key = (sender_no_10, receiver_no_8)
            groups.setdefault(key, []).append(l)

        project_root = BASE_DIR  # xsd は BASE_DIR/xsd

        for (sender_no_10, receiver_no_8), rows in groups.items():
            if not rows:
                continue

            # ルートフォルダ（厚労省固定）
            root_dir_name = group_dirname(sender_no_10, receiver_no_8, file_date)
            out_dir = export_root_path / root_dir_name
            data_dir = out_dir / "DATA"
            xsd_dir = out_dir / "XSD"
            data_dir.mkdir(parents=True, exist_ok=True)

            # XSD copy（必須）
            copy_xsd_folder(project_root, xsd_dir)

            # personal XMLs（厚労省ファイル名固定）
            seq = 1
            for lr in rows:
                items = fetch_items_for_ledger(conn, lr.ledger_id)
                tree = build_clinical_document_xml(lr, items, file_date)

                fname = person_xml_filename(sender_no_10, file_date, seq)
                write_xml(tree, data_dir / fname, encoding=encoding)
                seq += 1

            total = len(rows)

            # IX08
            ix08_tree = build_ix08_xml(
                sender_extension=str(sender_no_10),
                file_date=file_date,
                total_count=total,
                receiver_extension=str(receiver_no_8),
            )
            ix08_path = out_dir / ix08_filename()
            write_xml(ix08_tree, ix08_path, encoding=encoding)

            # ZIPは「ルートフォルダと同列」に出す（ZIPの中身はルートフォルダごと）
            zname = zip_filename(root_dir_name)
            zip_path = export_root_path / zname
            make_zip_from_export_root(zip_path, export_root_path, include_root_dir_name=root_dir_name)

            print(f"[OK] sender={sender_no_10} receiver={receiver_no_8} count={total}")
            print(f"     root_dir={out_dir}")
            print(f"     zip={zip_path}")

        print(f"[DONE] file_date={file_date} encoding={encoding} export_root={export_root_path}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
