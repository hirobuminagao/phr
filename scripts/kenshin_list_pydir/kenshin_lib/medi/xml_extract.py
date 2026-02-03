# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/xml_extract.py

medi_xml_receipts の target_status を対象に、ZIPから該当XMLを取り出し
- well-formed
- CDA document_id 抽出（OK/SKIP/ERROR）
- XSD validate（ログのみ。抽出自体は継続。XSDが無い場合はSKIP）
- items 抽出（※欠損があっても継続。warning として process_log に残す）
- medi_xml_ledger へ反映
を実施し、index/台帳を更新する。

NOTE:
- medi_xml_ledger に zip_inner_path_sha256 が NOT NULL の環境があるため、
  xml_extract 側でも必ず算出して渡す（DB側が列を持つなら保存される）。
- medi_xml_process_logs.step が ENUM 等で制限されている可能性があるので、
  step は短い固定値に寄せる。

FIX (2026-01-15):
- encrypted ZIP member に遭遇すると zipfile.open が RuntimeError を投げて run が落ちるため、
  medi_zip_passwords から候補を取り、候補を順に試して継続できるようにした。

POLICY (2026-01-20):
- postal_code / gender_code 等の “人情報欠損” では突き返さない（ここは原本保持レイヤー）。
  欠損は warning としてログへ残し、ledger には NULL のまま格納して継続する。
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Optional, Iterable

import os
import inspect
import hashlib
import zipfile
import xml.etree.ElementTree as ET
from lxml import etree

from kenshin_lib.medi.db_medi import (
    db_select_pending_xmls,
    db_get_zip_receipt_row_by_sha,
    db_insert_xml_process_log,
    db_update_xml_index_fields,
    db_upsert_xml_ledger,
)

from kenshin_lib.medi.zip_passwords import get_password_candidates


NS_HL7 = {"hl7": "urn:hl7-org:v3"}
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"

_XSD_CACHE: dict[str, etree.XMLSchema] = {}

# step は DB 側で enum 制限がある想定で、短い固定値に寄せる
STEP_WELLFORMED = "WELLFORMED"
STEP_CDA_INDEX = "CDA_INDEX"
STEP_XSD_VALIDATE = "XSD_VALIDATE"
STEP_EXTRACT_ITEMS = "EXTRACT_ITEMS"
STEP_LEDGER = "LEDGER"


# ------------------------------------------------------------
# small utils
# ------------------------------------------------------------
def _shorten(s: str, max_len: int = 2000) -> str:
    s2 = (s or "").replace("\r", " ").replace("\n", " ").strip()
    return s2 if len(s2) <= max_len else s2[: max_len - 3] + "..."


def _safe_call(fn, /, *args, **kwargs):
    """
    db_medi 側の関数シグネチャ揺れを吸収するため、
    signature を見て渡せる kwargs だけ渡す。
    """
    sig = inspect.signature(fn)
    allowed = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    return fn(*args, **filtered)


def _default_base_dir() -> Path:
    # .../kenshin_list_pydir/kenshin_lib/medi/xml_extract.py -> parents[2] = kenshin_list_pydir
    return Path(__file__).resolve().parents[2]


def _resolve_path(base_dir: Path, raw: str) -> Path:
    p = Path(raw)
    return (base_dir / p).resolve() if not p.is_absolute() else p


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _norm_inner_path(inner_path: str) -> str:
    # ZIP内パスは / に寄せて同一化
    return (inner_path or "").replace("\\", "/").lstrip("/")


# ------------------------------------------------------------
# XSD loader / validator
# ------------------------------------------------------------
def _load_schema(xsd_main_path: Path) -> etree.XMLSchema:
    key = str(xsd_main_path.resolve())
    if key in _XSD_CACHE:
        return _XSD_CACHE[key]
    parser = etree.XMLParser(load_dtd=False, no_network=True, resolve_entities=False, huge_tree=True)
    with xsd_main_path.open("rb") as f:
        xsd_doc = etree.parse(f, parser)
    schema = etree.XMLSchema(xsd_doc)
    _XSD_CACHE[key] = schema
    return schema


def _schema_location_xsd_name(xml_bytes: bytes) -> Optional[str]:
    try:
        root = ET.fromstring(xml_bytes)
        schema_loc = root.attrib.get(f"{{{NS_XSI}}}schemaLocation")
        if not schema_loc:
            return None
        toks = schema_loc.split()
        candidates = [t for t in toks if t.lower().endswith(".xsd")]
        if not candidates:
            return None
        return Path(candidates[-1]).name
    except Exception:
        return None


def validate_xsd(xml_bytes: bytes, xsd_root: Path, xsd_main_default: str) -> tuple[bool, Optional[str], str]:
    """
    戻り: (ok, used_xsd_filename, message)
    - schemaLocation が指す xsd がローカルにあれば優先
    - 無ければ xsd_main_default を使う
    - xsd が無ければ SKIP 扱いで (True, used, "XSD file not found (skip)")
    """
    used = None
    name = _schema_location_xsd_name(xml_bytes)
    if name and (xsd_root / name).exists():
        used = name
        xsd_main_path = xsd_root / name
    else:
        used = xsd_main_default
        xsd_main_path = xsd_root / xsd_main_default

    if not xsd_main_path.exists():
        return True, used, "XSD file not found (skip)"

    try:
        schema = _load_schema(xsd_main_path)
        parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)
        doc = etree.fromstring(xml_bytes, parser)
        ok = schema.validate(doc)
        if ok:
            return True, used, ""
        errs = schema.error_log
        msg = "; ".join([f"{e.line}:{_shorten(e.message, 300)}" for e in list(errs)[:3]])
        return False, used, msg or "XSD validation failed"
    except Exception as e:
        return False, used, f"XSD validator exception: {_shorten(str(e), 500)}"


# ------------------------------------------------------------
# CDA helpers
# ------------------------------------------------------------
def _extract_document_id(tree: etree._ElementTree) -> tuple[Optional[str], str]:
    """
    CDA_INDEX: document_id を抽出
    戻り: (document_id, result) result in {'OK','SKIP','ERROR'}
    """
    try:
        ids = tree.xpath("/hl7:ClinicalDocument/hl7:id", namespaces=NS_HL7)
        id_node = ids[0] if ids else None
        if id_node is None:
            return None, "ERROR"

        root = id_node.get("root")
        ext = id_node.get("extension")
        nf = id_node.get("nullFlavor")

        if root:
            return f"{root}|{ext}" if ext else root, "OK"
        if nf:
            return None, "SKIP"
        return None, "SKIP"
    except Exception:
        return None, "ERROR"


def _get_attr(tree: etree._ElementTree, xpath_expr: str, attr_name: str) -> str:
    r = tree.xpath(xpath_expr, namespaces=NS_HL7)
    if r and hasattr(r[0], "get"):
        return r[0].get(attr_name, "") or ""
    return ""


def _get_text(tree: etree._ElementTree, xpath_expr: str) -> str:
    r = tree.xpath(xpath_expr, namespaces=NS_HL7)
    if not r:
        return ""
    node = r[0]
    t = (node.text or "") if hasattr(node, "text") else str(node)
    return " ".join(t.split())


def _try_get_attr(tree: etree._ElementTree, xpath_list: list[str], attr_name: str) -> str:
    for xp in xpath_list:
        v = _get_attr(tree, xp, attr_name)
        if v:
            return v
    return ""


def _format_yyyymmdd(s: str) -> Optional[dt.date]:
    if not s:
        return None
    s2 = s.strip()
    if len(s2) == 8 and s2.isdigit():
        try:
            return dt.datetime.strptime(s2, "%Y%m%d").date()
        except Exception:
            return None
    return None


def _extract_address(tree: etree._ElementTree) -> tuple[str, str]:
    postal = _get_text(tree, "//hl7:recordTarget/hl7:patientRole/hl7:addr/hl7:postalCode")
    parts = []
    for xp in [
        "//hl7:recordTarget/hl7:patientRole/hl7:addr/hl7:state",
        "//hl7:recordTarget/hl7:patientRole/hl7:addr/hl7:city",
        "//hl7:recordTarget/hl7:patientRole/hl7:addr/hl7:streetAddressLine",
    ]:
        t = _get_text(tree, xp)
        if t:
            parts.append(t)
    addr = " ".join(parts)
    return postal, addr


def _extract_insurance_ids(tree: etree._ElementTree) -> tuple[str, str, str]:
    insurance_symbol = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.204']",
        "extension",
    )
    insurance_number = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.205']",
        "extension",
    )
    insurance_branch_number = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.211']",
        "extension",
    )
    return insurance_symbol, insurance_number, insurance_branch_number


def _extract_items(tree: etree._ElementTree) -> dict:
    gender_code = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:administrativeGenderCode",
        "code",
    )

    postal_code, address = _extract_address(tree)

    facility_name = _get_text(
        tree,
        "//hl7:documentationOf/hl7:serviceEvent/hl7:performer/hl7:assignedEntity/hl7:representedOrganization/hl7:name",
    )
    facility_code = _get_attr(
        tree,
        "//hl7:documentationOf/hl7:serviceEvent/hl7:performer/hl7:assignedEntity/hl7:representedOrganization/hl7:id[@root='1.2.392.200119.6.102']",
        "extension",
    )

    insurer_number = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:id[@root='1.2.392.200119.6.101']",
        "extension",
    )
    insurance_symbol, insurance_number, insurance_branch_number = _extract_insurance_ids(tree)

    birth_raw = _get_attr(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:birthTime",
        "value",
    )
    exam_raw = _try_get_attr(
        tree,
        [
            "//hl7:documentationOf/hl7:serviceEvent/hl7:effectiveTime",
            "//hl7:documentationOf/hl7:serviceEvent/hl7:effectiveTime/hl7:low",
        ],
        "value",
    )

    birthdate = _format_yyyymmdd(birth_raw)
    exam_date = _format_yyyymmdd(exam_raw)

    patient_name = _get_text(
        tree,
        "//hl7:recordTarget/hl7:patientRole/hl7:patient/hl7:name",
    )

    return {
        "gender_code": gender_code,
        "postal_code": postal_code,
        "address": address,
        "facility_code": facility_code,
        "facility_name": facility_name,
        "insurer_number": insurer_number,
        "insurance_symbol": insurance_symbol,
        "insurance_number": insurance_number,
        "insurance_branch_number": insurance_branch_number,
        "birthdate": birthdate,
        "exam_date": exam_date,
        "patient_name": patient_name,
        "raw_birth_yyyymmdd": birth_raw,
        "raw_exam_yyyymmdd": exam_raw,
    }


def _build_missing_message(*, items: dict) -> Optional[str]:
    """
    “落とさない” けど品質として残したい欠損を warning として整形する。
    必須扱いはしない（ここは原本保持レイヤー）。
    """
    missing: list[str] = []
    if not (items.get("gender_code") or "").strip():
        missing.append("gender_code")
    if not (items.get("postal_code") or "").strip():
        missing.append("postal_code")
    # ここに増やしたければ追加（例: birthdate/exam_date など）
    if not missing:
        return None
    return "warning missing: " + ",".join(missing)


# ------------------------------------------------------------
# ZIP member read (password-aware)
# ------------------------------------------------------------
def _open_member_bytes(
    zf: zipfile.ZipFile,
    inner_path: str,
    *,
    pwd_candidates: Optional[Iterable[str]] = None,
) -> Optional[bytes]:
    """
    ZIP内ファイルを bytes で読む。

    - まず通常で試す
    - KeyError の場合は “末尾一致” で候補が1つだけならそれを読む（既存互換）
    - encrypted の場合は pwd_candidates を順に試す（pwd は bytes で zipfile.open に渡す）
    """
    norm = _norm_inner_path(inner_path)

    def _try_open(name: str, pwd: Optional[bytes] = None) -> bytes:
        # zipfile.ZipFile.open は pwd を渡せる
        with zf.open(name, "r", pwd=pwd) as fp:
            return fp.read()

    # 末尾一致救済のターゲット決定
    targets = [norm]
    try:
        _ = zf.getinfo(norm)
    except KeyError:
        cands = [n for n in zf.namelist() if n.endswith("/" + norm) or n.endswith(norm)]
        if len(cands) == 1:
            targets = [cands[0]]
        elif len(cands) >= 2:
            targets = cands[:5]  # “全滅よりマシ”枠

    # 1) 通常（pwdなし）
    try:
        for t in targets:
            try:
                return _try_open(t, pwd=None)
            except KeyError:
                continue
        return None
    except RuntimeError as e:
        # 暗号 or それ以外
        msg = str(e).lower()
        if "password required" not in msg and "encrypted" not in msg:
            raise

    # 2) 暗号: 候補を試す
    if not pwd_candidates:
        raise RuntimeError(f"zip member is encrypted and no password candidates: {norm}")

    last_err: Optional[Exception] = None
    for pw in pwd_candidates:
        try:
            pw_bytes = (pw or "").encode("utf-8")
            for t in targets:
                try:
                    return _try_open(t, pwd=pw_bytes)
                except KeyError:
                    continue
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise RuntimeError(f"password candidates exhausted for {norm}: {last_err}") from last_err
    raise RuntimeError(f"password candidates exhausted for {norm}")


# ------------------------------------------------------------
# Config (optional)
# ------------------------------------------------------------
@dataclass(frozen=True)
class XmlExtractRuntime:
    base_dir: Path
    xsd_root: Optional[Path]
    xsd_main_default: str


def _load_runtime_from_env(base_dir: Path) -> XmlExtractRuntime:
    xsd_root_raw = os.getenv("MEDI_IMPORT_XSD_ROOT", "").strip()
    xsd_root = _resolve_path(base_dir, xsd_root_raw) if xsd_root_raw else None
    xsd_main_default = os.getenv("MEDI_IMPORT_XSD_MAIN", "").strip() or "hc08_V08.xsd"
    return XmlExtractRuntime(base_dir=base_dir, xsd_root=xsd_root, xsd_main_default=xsd_main_default)


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------
def xml_extract_phase(logger, cur, *, run_id: int, target_status: str, limit: int) -> tuple[int, int, int]:
    base_dir = _default_base_dir()
    rt = _load_runtime_from_env(base_dir)

    rows = _safe_call(
        db_select_pending_xmls,
        cur,
        target_status=target_status,
        status=target_status,  # 旧名吸収
        limit=limit,
    )
    if not rows:
        logger.info("[EXTRACT] no target rows")
        return (0, 0, 0)

    processed = ok = err = 0
    zip_cache: dict[str, zipfile.ZipFile] = {}
    zip_row_cache: dict[str, dict] = {}
    zip_pw_cache: dict[str, list[str]] = {}

    try:
        for r in rows:
            processed += 1

            xml_sha = r.get("xml_sha256") or r.get("xml_sha") or r.get("sha256") or ""
            zip_sha = r.get("zip_sha256") or r.get("zip_sha") or ""
            inner_raw = r.get("zip_inner_path") or r.get("inner_path") or ""
            inner = _norm_inner_path(inner_raw)

            inner_sha = r.get("zip_inner_path_sha256") or (_sha256_text(inner) if inner else None)
            xml_receipt_id = r.get("xml_receipt_id") or r.get("id") or r.get("xml_id")

            if not xml_sha or not zip_sha or not inner:
                msg = f"row missing key(s): xml_sha={bool(xml_sha)} zip_sha={bool(zip_sha)} inner={bool(inner)}"
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha or ("0" * 64),
                    step=STEP_WELLFORMED,
                    result="ERROR",
                    message=_shorten(msg, 1000),
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="ROW_KEY_MISSING",
                    error_message=_shorten(msg, 1000),
                    document_id=None,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            if not inner_sha:
                inner_sha = _sha256_text(inner)

            # zip receipt row
            zrow = zip_row_cache.get(zip_sha)
            if zrow is None:
                zrow = _safe_call(db_get_zip_receipt_row_by_sha, cur, zip_sha256=zip_sha)
                if not zrow or not zrow.get("zip_path"):
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_WELLFORMED,
                        result="ERROR",
                        message="parent zip not found in medi_zip_receipts",
                    )
                    _safe_call(
                        db_update_xml_index_fields,
                        cur,
                        xml_sha256=xml_sha,
                        xml_receipt_id=xml_receipt_id,
                        status="ERROR",
                        error_code="PARENT_ZIP_MISSING",
                        error_message="parent zip not found",
                        document_id=None,
                        extracted_run_id=None,
                        extracted_at_now=False,
                    )
                    err += 1
                    continue
                zip_row_cache[zip_sha] = zrow

            zip_path = str(zrow["zip_path"])

            # zip open cache
            zf = zip_cache.get(zip_sha)
            if zf is None:
                zf = zipfile.ZipFile(zip_path, "r")
                zip_cache[zip_sha] = zf

            # password candidates (zip単位で1回だけDBから取る)
            pwd_candidates = zip_pw_cache.get(zip_sha)
            if pwd_candidates is None:
                try:
                    pwd_candidates = get_password_candidates(
                        cur,
                        facility_code=str(zrow.get("facility_code") or ""),
                        facility_folder_name=str(zrow.get("facility_folder_name") or ""),
                        zip_name=str(zrow.get("zip_name") or ""),
                        zip_sha256=zip_sha,
                    )
                except Exception:
                    pwd_candidates = []
                zip_pw_cache[zip_sha] = list(pwd_candidates or [])

            # read xml bytes (password-aware)
            try:
                b = _open_member_bytes(zf, inner, pwd_candidates=pwd_candidates)
            except RuntimeError as e:
                msg = _shorten(str(e), 1200)
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_WELLFORMED,
                    result="ERROR",
                    message=_shorten(f"zip open failed: {msg}", 1500),
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="ZIP_PASSWORD" if "password" in str(e).lower() else "ZIP_OPEN",
                    error_message=msg,
                    document_id=None,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            if b is None:
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_WELLFORMED,
                    result="ERROR",
                    message=f"zip member not found: {inner}",
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="ZIP_MEMBER_NOT_FOUND",
                    error_message=_shorten(f"zip member not found: {inner}", 1000),
                    document_id=None,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            # 1) well-formed (ElementTree)
            try:
                ET.fromstring(b)
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_WELLFORMED,
                    result="OK",
                    message=None,
                )
            except Exception as e:
                msg = _shorten(str(e), 1000)
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_WELLFORMED,
                    result="ERROR",
                    message=msg,
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="XML_PARSE",
                    error_message=msg,
                    document_id=None,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            # parse with lxml
            try:
                parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)
                root = etree.fromstring(b, parser)
                tree = etree.ElementTree(root)
            except Exception as e:
                msg = _shorten(str(e), 1000)
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_WELLFORMED,
                    result="ERROR",
                    message=f"lxml parse: {msg}",
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="XML_PARSE_LXML",
                    error_message=msg,
                    document_id=None,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            # 2) CDA index
            doc_id: Optional[str] = None
            try:
                doc_id, cda_result = _extract_document_id(tree)
                if cda_result == "OK":
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_CDA_INDEX,
                        result="OK",
                        message=None,
                    )
                elif cda_result == "SKIP":
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_CDA_INDEX,
                        result="SKIP",
                        message="id root missing or nullFlavor (allowed)",
                    )
                    doc_id = None
                else:
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_CDA_INDEX,
                        result="ERROR",
                        message="unexpected CDA index error",
                    )
                    doc_id = None
            except Exception as e:
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_CDA_INDEX,
                    result="ERROR",
                    message=_shorten(str(e), 1000),
                )
                doc_id = None

            # 3) XSD validate（ログのみ。抽出は継続）
            xsd_valid: Optional[int] = None
            xsd_note: Optional[str] = None

            if rt.xsd_root and rt.xsd_root.exists():
                xsd_ok, used_xsd, xsd_msg = validate_xsd(b, rt.xsd_root, rt.xsd_main_default)
                msg_lower = (xsd_msg or "").strip().lower()

                if msg_lower.startswith("xsd file not found"):
                    xsd_valid = None
                    xsd_note = _shorten(f"used={used_xsd} {xsd_msg}", 1500)
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_XSD_VALIDATE,
                        result="SKIP",
                        message=xsd_note,
                    )
                elif xsd_ok:
                    xsd_valid = 1
                    xsd_note = (f"used={used_xsd}" if used_xsd else None)
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_XSD_VALIDATE,
                        result="OK",
                        message=xsd_note,
                    )
                else:
                    xsd_valid = 0
                    xsd_note = _shorten(f"used={used_xsd} {xsd_msg}", 1500)
                    _safe_call(
                        db_insert_xml_process_log,
                        cur,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step=STEP_XSD_VALIDATE,
                        result="ERROR",
                        message=xsd_note,
                    )
            else:
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_XSD_VALIDATE,
                    result="SKIP",
                    message="xsd_root not set or not exists",
                )

            # 4) items extract（欠損でも落とさない）
            items = _extract_items(tree)

            warn_parts: list[str] = []

            miss_msg = _build_missing_message(items=items)
            if miss_msg:
                warn_parts.append(miss_msg)

            # facility は “台帳としての利便性” のため warning に残す（必須にはしない）
            if not items.get("facility_code"):
                warn_parts.append("warning missing: facility_code")
            if not items.get("facility_name"):
                warn_parts.append("warning missing: facility_name")

            wmsg = "; ".join(warn_parts) if warn_parts else None

            _safe_call(
                db_insert_xml_process_log,
                cur,
                run_id=run_id,
                xml_sha256=xml_sha,
                step=STEP_EXTRACT_ITEMS,
                result="OK",
                message=_shorten(wmsg, 1000) if wmsg else None,
            )

            # 5) ledger upsert（warning があっても実施）
            try:
                xml_filename = Path(inner).name

                _safe_call(
                    db_upsert_xml_ledger,
                    cur,
                    run_id=run_id,
                    zip_receipt_id=int(zrow["zip_receipt_id"]),
                    facility_folder_name=zrow.get("facility_folder_name"),
                    facility_code=zrow.get("facility_code"),
                    facility_name=zrow.get("facility_name"),
                    zip_name=str(zrow.get("zip_name") or ""),
                    zip_sha256=zip_sha,
                    xml_filename=xml_filename,
                    zip_inner_path=inner,
                    zip_inner_path_sha256=inner_sha,
                    insurer_number=items.get("insurer_number") or None,
                    insurance_symbol=items.get("insurance_symbol") or None,
                    insurance_number=items.get("insurance_number") or None,
                    insurance_branch_number=items.get("insurance_branch_number") or None,
                    birth_date=items.get("birthdate"),
                    kenshin_date=items.get("exam_date"),
                    gender_code=items.get("gender_code") or None,
                    name_kana_full=items.get("patient_name") or None,
                    postal_code=items.get("postal_code") or None,
                    address=items.get("address") or None,
                    org_name_in_xml=items.get("facility_name") or None,
                    org_code_in_xml=items.get("facility_code") or None,
                    report_category_code=None,
                    program_type_code=None,
                    guidance_level_code=None,
                    metabo_code=None,
                    xsd_valid=xsd_valid,
                    error_content=xsd_note,
                )

                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_LEDGER,
                    result="OK",
                    message=None,
                )
            except Exception as e:
                msg = _shorten(str(e), 1200)
                _safe_call(
                    db_insert_xml_process_log,
                    cur,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step=STEP_LEDGER,
                    result="ERROR",
                    message=msg,
                )
                _safe_call(
                    db_update_xml_index_fields,
                    cur,
                    xml_sha256=xml_sha,
                    xml_receipt_id=xml_receipt_id,
                    status="ERROR",
                    error_code="LEDGER_UPSERT",
                    error_message=msg,
                    document_id=doc_id,
                    extracted_run_id=None,
                    extracted_at_now=False,
                )
                err += 1
                continue

            # OK update（warning があっても OK。品質は process_logs に残す）
            _safe_call(
                db_update_xml_index_fields,
                cur,
                xml_sha256=xml_sha,
                xml_receipt_id=xml_receipt_id,
                status="OK",
                error_code=None,
                error_message=None,
                document_id=doc_id,
                extracted_run_id=run_id,
                extracted_at_now=True,
            )
            ok += 1

    finally:
        for zf in zip_cache.values():
            try:
                zf.close()
            except Exception:
                pass

    return (processed, ok, err)
