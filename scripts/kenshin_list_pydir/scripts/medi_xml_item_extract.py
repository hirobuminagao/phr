# -*- coding: utf-8 -*-
"""
scripts/medi_xml_item_extract.py

【方針（2026-01-21 修正版）】
work_other.medi_xml_receipts を対象に、ZIPからXMLを取り出して

(1) 原本レイヤーとして「値っぽいもの」をまず全部拾って medi_xml_item_values に記帳する（=ベース抽出）
    - 観測対象: CDAの observation を中心に、code/@code を namecode として採用
    - namecode が取れないもの（code欠損）は “何の検査か判別不能” なのでスキップ
    - value は observation/value を基本に拾う（value が無ければ text なども一応拾う）

(2) その上で、dev_phr.exam_item_master に当該 namecode が存在する場合は
    - value_method / xml_value_type などの “評価・整形ヒント” を反映して書き込む
    - xpath_template は「構造判定（追加抽出）用途」として別モードで使えるよう余地を残す
      ※今回の事故（written=0）を避けるため、xpath_template だけに依存しない

テーブル:
- work_other.medi_xml_item_values
  UNIQUE(xml_sha256, namecode, occurrence_no)

実行結果:
- medi_xml_receipts.items_extract_status / items_extracted_* を更新
- medi_xml_process_logs に step=EXTRACT_ITEMS を記帳（written=xxx）

ENV:
  # work_other(medi) 接続（.envの既存キーを正にする）
  MEDI_IMPORT_DB_HOST
  MEDI_IMPORT_DB_PORT
  MEDI_IMPORT_DB_USER
  MEDI_IMPORT_DB_PASSWORD
  MEDI_IMPORT_DB_NAME (default: work_other)

  # dev_phr 接続（2本目）
  PHR_MYSQL_HOST (空なら MEDI_IMPORT_DB_HOST を流用)
  PHR_MYSQL_PORT (空なら MEDI_IMPORT_DB_PORT を流用)
  PHR_MYSQL_USER
  PHR_MYSQL_PASSWORD
  PHR_MYSQL_DB (default: dev_phr)

  # runtime
  ITEM_EXTRACT_LIMIT (default 200)
    - limit > 0 : 件数制限あり
    - limit <= 0: 全件（LIMIT句なし）
  ITEM_EXTRACT_RUN_ID (default 0)
    - 0/未指定: このスクリプトが run を起票する
    - >0: 既存run_idを利用（存在しない場合はエラー）
  ITEM_EXTRACT_NOTE (optional)

  # 追加: 暗号ZIP対応（xml_extract と同様に候補をDBから引く）
  ITEM_EXTRACT_ZIP_PASSWORD_ENABLED (default true)
"""

from __future__ import annotations

# --- path bootstrap (MUST be before importing kenshin_lib) ---
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # = kenshin_list_pydir
sys.path.insert(0, str(BASE_DIR))
# ------------------------------------------------------------

import os
import hashlib
import zipfile
from typing import Optional, Iterable

from dotenv import load_dotenv
from lxml import etree

import mysql.connector
from mysql.connector.cursor import MySQLCursorDict

from kenshin_lib.medi.db_medi import (
    db_insert_run,
    db_finish_run,
    db_select_target_xmls_for_item_extract,
    db_get_zip_receipt_row_by_sha,
    db_insert_xml_process_log,
    db_update_items_extract_fields,
    db_upsert_xml_item_value,
)

from kenshin_lib.phr.db_phr import db_select_exam_items
from kenshin_lib.medi.zip_passwords import get_password_candidates


# CDA namespace（DBのxpath_templateは cda: を想定しているケースが多い）
NS_CDA = {"cda": "urn:hl7-org:v3", "hl7": "urn:hl7-org:v3"}
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"


# ------------------------------------------------------------
# env utils (medi_zip_import と同じ作法)
# ------------------------------------------------------------
def env_required(key: str) -> str:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        raise RuntimeError(f"必須環境変数 {key} が設定されていません")
    return v.strip()


def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# ------------------------------------------------------------
# small utils
# ------------------------------------------------------------
def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _norm_inner_path(p: str) -> str:
    return (p or "").replace("\\", "/").lstrip("/")


def _shorten(s: str, max_len: int = 2000) -> str:
    s2 = (s or "").replace("\r", " ").replace("\n", " ").strip()
    return s2 if len(s2) <= max_len else s2[: max_len - 3] + "..."


def _strip_or_none(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = str(s).strip()
    return s2 if s2 != "" else None


def _localname_of(elem) -> Optional[str]:
    """
    ★Pylance対策:
    etree.QName は endswith できないので localname で比較する。
    """
    try:
        return etree.QName(elem).localname  # type: ignore[arg-type]
    except Exception:
        return None


def _is_cda_clinical_document(root) -> bool:
    """
    CDAっぽいXMLかの雑チェック（強制ではない）。
    - ルートが ClinicalDocument かどうか
    """
    ln = _localname_of(root)
    return ln == "ClinicalDocument"


# ------------------------------------------------------------
# DB connect helpers
# ------------------------------------------------------------
def load_medi_db_params() -> dict:
    host = env_required("MEDI_IMPORT_DB_HOST")
    port = int(env_required("MEDI_IMPORT_DB_PORT"))
    name = os.getenv("MEDI_IMPORT_DB_NAME", "work_other").strip() or "work_other"
    user = env_required("MEDI_IMPORT_DB_USER")
    password = env_required("MEDI_IMPORT_DB_PASSWORD")

    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": False,
        "use_pure": True,
    }


def load_phr_db_params(*, fallback_host: str, fallback_port: int) -> dict:
    host = os.getenv("PHR_MYSQL_HOST", "").strip() or fallback_host
    port = int((os.getenv("PHR_MYSQL_PORT", str(fallback_port)).strip() or str(fallback_port)))
    name = os.getenv("PHR_MYSQL_DB", "dev_phr").strip() or "dev_phr"
    user = env_required("PHR_MYSQL_USER")
    password = env_required("PHR_MYSQL_PASSWORD")

    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": False,
        "use_pure": True,
    }


def connect_mysql(params: dict):
    return mysql.connector.connect(**params)


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


def run_id_exists(cur, run_id: int) -> bool:
    cur.execute("SELECT 1 AS ok FROM medi_import_runs WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    return bool(row)


def select_targets_safe(cur_medi, *, limit: int) -> list[dict]:
    """
    db_select_target_xmls_for_item_extract は LIMIT %s を固定で持つ前提なので、
    limit<=0 のときは「十分でかい数」で取得し、全件相当として扱う。
    """
    if limit <= 0:
        limit = 1_000_000
    return db_select_target_xmls_for_item_extract(cur_medi, limit=limit, target_status="OK")


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

    - まず通常で試す（pwdなし）
    - encrypted の場合は pwd_candidates を順に試す
    - inner_path が完全一致しない場合、末尾一致で救済（先頭5まで）
    """
    inner = _norm_inner_path(inner_path)

    def _try_open(name: str, pwd: Optional[bytes] = None) -> bytes:
        with zf.open(name, "r", pwd=pwd) as fp:
            return fp.read()

    targets = [inner]
    try:
        _ = zf.getinfo(inner)
    except KeyError:
        cands = [n for n in zf.namelist() if n.endswith("/" + inner) or n.endswith(inner)]
        if len(cands) == 1:
            targets = [cands[0]]
        elif len(cands) >= 2:
            targets = cands[:5]

    # 1) 通常
    try:
        for t in targets:
            try:
                return _try_open(t, pwd=None)
            except KeyError:
                continue
        return None
    except RuntimeError as e:
        msg = str(e).lower()
        if "password required" not in msg and "encrypted" not in msg:
            raise

    # 2) 暗号: pwd候補
    if not pwd_candidates:
        raise RuntimeError(f"zip member is encrypted and no password candidates: {inner}")

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
        raise RuntimeError(f"password candidates exhausted for {inner}: {last_err}") from last_err
    raise RuntimeError(f"password candidates exhausted for {inner}")


# ------------------------------------------------------------
# XML value extraction helpers
# ------------------------------------------------------------
def _xsi_type(node) -> Optional[str]:
    if not hasattr(node, "get"):
        return None
    v = node.get(f"{{{NS_XSI}}}type")
    return _strip_or_none(v)


def _infer_value_type_from_node(vnode) -> Optional[str]:
    xt = _xsi_type(vnode)
    if xt:
        return xt

    if hasattr(vnode, "get") and vnode.get("value") is not None:
        return "ST"
    txt = (vnode.text or "").strip() if hasattr(vnode, "text") else ""
    if txt:
        return "ST"
    return None


def _extract_by_value_method(node, value_method: str) -> Optional[str]:
    vm = (value_method or "").strip()

    if not vm:
        if hasattr(node, "get"):
            v = node.get("value")
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        t = (node.text or "").strip() if hasattr(node, "text") else ""
        return t if t != "" else None

    if vm.startswith("@"):
        attr = vm[1:]
        if hasattr(node, "get"):
            v = node.get(attr)
            return (str(v).strip() if v is not None and str(v).strip() != "" else None)
        return None

    if vm in ("text()", "text"):
        t = (node.text or "").strip() if hasattr(node, "text") else ""
        return t if t != "" else None

    if vm in ("string()", "string"):
        if hasattr(node, "itertext"):
            t = "".join([t for t in node.itertext()]).strip()
            return t if t != "" else None
        return None

    t = (node.text or "").strip() if hasattr(node, "text") else ""
    return t if t != "" else None


def _infer_value_type_from_master(xml_value_type: str) -> Optional[str]:
    t = (xml_value_type or "").strip().upper()
    if t in ("PQ", "CD", "CO", "ST"):
        return t
    return None


def _value_from_value_node(
    vnode,
    *,
    value_method: str = "",
    prefer_master_type: Optional[str] = None,
) -> tuple[
    Optional[str],  # value_raw
    Optional[str],  # value_type
    Optional[str],  # unit
    Optional[str],  # code_system
    Optional[str],  # code_value
    Optional[str],  # code_display
]:
    """
    ★戻り値は必ず6要素タプル（Noneでも）に統一
    """
    if vnode is None:
        return (None, None, None, None, None, None)

    value_raw = _extract_by_value_method(vnode, value_method)

    unit: Optional[str] = None
    code_system: Optional[str] = None
    code_value: Optional[str] = None
    code_display: Optional[str] = None

    if hasattr(vnode, "get"):
        unit = _strip_or_none(vnode.get("unit"))
        code_system = _strip_or_none(vnode.get("codeSystem"))
        code_value = _strip_or_none(vnode.get("code"))
        code_display = _strip_or_none(vnode.get("displayName"))

    value_type = prefer_master_type or _infer_value_type_from_node(vnode)

    return (value_raw, value_type, unit, code_system, code_value, code_display)


def _collect_observations_as_raw_items(
    tree: etree._ElementTree,
    *,
    item_master_map: dict[str, dict],
) -> list[dict]:
    """
    observationを走査して、namecode=code/@code をキーに「値っぽいもの」を全部拾う。
    """
    out: list[dict] = []

    obs_nodes = tree.xpath("//cda:observation", namespaces=NS_CDA)

    occ: dict[str, int] = {}

    for obs in obs_nodes:
        try:
            code_list = obs.xpath("./cda:code", namespaces=NS_CDA)
            code_node = code_list[0] if code_list else None
            if code_node is None or not hasattr(code_node, "get"):
                continue

            namecode = _strip_or_none(code_node.get("code"))
            if not namecode:
                continue

            vlist = obs.xpath("./cda:value", namespaces=NS_CDA)
            vnode = vlist[0] if vlist else None

            if vnode is None:
                tlist = obs.xpath("./cda:text", namespaces=NS_CDA)
                vnode = tlist[0] if tlist else None

            master = item_master_map.get(namecode) or {}
            prefer_type = _infer_value_type_from_master(master.get("xml_value_type") or "")
            value_method = (master.get("value_method") or "").strip()

            value_raw, value_type, unit, v_code_system, v_code_value, v_code_display = _value_from_value_node(
                vnode,
                value_method=value_method,
                prefer_master_type=prefer_type,
            )

            occ[namecode] = occ.get(namecode, 0) + 1
            occurrence_no = occ[namecode]

            obs_code_system = _strip_or_none(code_node.get("codeSystem"))
            obs_code_value = _strip_or_none(code_node.get("code"))
            obs_code_display = _strip_or_none(code_node.get("displayName"))

            code_system = v_code_system or obs_code_system
            code_value = v_code_value or obs_code_value
            code_display = v_code_display or obs_code_display

            out.append(
                {
                    "namecode": namecode,
                    "occurrence_no": occurrence_no,
                    "value_raw": value_raw,
                    "value_type": value_type,
                    "unit": unit,
                    "code_system": code_system,
                    "code_value": code_value,
                    "code_display": code_display,
                }
            )
        except Exception:
            continue

    return out


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
def main() -> int:
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f".env loaded: {env_path}")

    medi_params = load_medi_db_params()
    phr_params = load_phr_db_params(fallback_host=medi_params["host"], fallback_port=medi_params["port"])

    limit = env_int("ITEM_EXTRACT_LIMIT", 200)
    env_run_id = env_int("ITEM_EXTRACT_RUN_ID", 0)
    note = os.getenv("ITEM_EXTRACT_NOTE", "").strip() or None
    zip_pw_enabled = env_bool("ITEM_EXTRACT_ZIP_PASSWORD_ENABLED", True)

    print(f"[ITEM_EXTRACT] LIMIT={limit}")
    print(f"[ITEM_EXTRACT] ZIP_PASSWORD_ENABLED={zip_pw_enabled}")
    print(f"[ITEM_EXTRACT] MEDI_DB={medi_params['host']}:{medi_params['port']}/{medi_params['database']} user={medi_params['user']}")
    print(f"[ITEM_EXTRACT] PHR_DB ={phr_params['host']}:{phr_params['port']}/{phr_params['database']} user={phr_params['user']}")
    print(f"[ITEM_EXTRACT] ENV_RUN_ID={env_run_id}")

    conn_medi = connect_mysql(medi_params)
    conn_phr = connect_mysql(phr_params)

    run_id: int = 0

    try:
        cur_medi = dict_cursor(conn_medi)
        cur_phr = dict_cursor(conn_phr)

        if env_run_id > 0:
            if not run_id_exists(cur_medi, env_run_id):
                raise RuntimeError(f"ITEM_EXTRACT_RUN_ID={env_run_id} が medi_import_runs に存在しません（FK整合性NG）")
            run_id = env_run_id
        else:
            input_root = str(BASE_DIR / "medi_input")
            note_prefix = note or "medi_xml_item_extract"
            run_id = int(db_insert_run(cur_medi, input_root, note_prefix))
            conn_medi.commit()

        print(f"[ITEM_EXTRACT] USING run_id={run_id}")

        items = db_select_exam_items(cur_phr, only_with_xpath=False)
        item_master_map = {i["namecode"]: i for i in items if i.get("namecode")}
        print(f"[ITEM_EXTRACT] item_master loaded: {len(item_master_map)} namecodes")

        targets = select_targets_safe(cur_medi, limit=limit)
        if not targets:
            db_finish_run(cur_medi, run_id, "item_extract: no targets (status=OK)")
            conn_medi.commit()
            print("[ITEM_EXTRACT] no targets")
            return 0

        processed = 0
        ok = 0
        err = 0
        zero_hit = 0

        zip_cache: dict[str, zipfile.ZipFile] = {}
        zip_row_cache: dict[str, dict] = {}
        zip_pw_cache: dict[str, list[str]] = {}

        for r in targets:
            processed += 1

            xml_receipt_id = int(r["xml_receipt_id"])
            xml_sha = str(r.get("xml_sha256") or "")
            zip_sha = str(r.get("zip_sha256") or "")
            inner = _norm_inner_path(str(r.get("zip_inner_path") or ""))

            if not xml_sha or not zip_sha or not inner:
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="ERROR",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                err += 1
                continue

            inner_sha = _sha256_text(inner)

            zrow = zip_row_cache.get(zip_sha)
            if zrow is None:
                zrow = db_get_zip_receipt_row_by_sha(cur_medi, zip_sha256=zip_sha)
                if not zrow or not zrow.get("zip_path"):
                    db_insert_xml_process_log(
                        cur_medi,
                        run_id=run_id,
                        xml_sha256=xml_sha,
                        step="EXTRACT_ITEMS",
                        result="ERROR",
                        message="item_extract: parent zip missing in medi_zip_receipts",
                    )
                    db_update_items_extract_fields(
                        cur_medi,
                        xml_receipt_id=xml_receipt_id,
                        items_extract_status="ERROR",
                        items_extracted_run_id=run_id,
                        items_extracted_at_now=True,
                    )
                    err += 1
                    continue
                zip_row_cache[zip_sha] = zrow

            zip_path = str(zrow["zip_path"])

            zf = zip_cache.get(zip_sha)
            if zf is None:
                zf = zipfile.ZipFile(zip_path, "r")
                zip_cache[zip_sha] = zf

            pwd_candidates: list[str] = []
            if zip_pw_enabled:
                cached = zip_pw_cache.get(zip_sha)
                if cached is None:
                    try:
                        cached = list(
                            get_password_candidates(
                                cur_medi,
                                facility_code=str(zrow.get("facility_code") or ""),
                                facility_folder_name=str(zrow.get("facility_folder_name") or ""),
                                zip_name=str(zrow.get("zip_name") or ""),
                                zip_sha256=zip_sha,
                            )
                            or []
                        )
                    except Exception:
                        cached = []
                    zip_pw_cache[zip_sha] = cached
                pwd_candidates = cached

            try:
                b = _open_member_bytes(zf, inner, pwd_candidates=pwd_candidates if zip_pw_enabled else None)
            except Exception as e:
                msg = _shorten(str(e), 1200)
                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result="ERROR",
                    message=_shorten(f"item_extract: zip open failed: {msg}", 1500),
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="ERROR",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                err += 1
                continue

            if not b:
                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result="ERROR",
                    message=f"item_extract: zip member not found: {inner}",
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="ERROR",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                err += 1
                continue

            try:
                parser = etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True)
                root = etree.fromstring(b, parser)
                tree = etree.ElementTree(root)
            except Exception as e:
                msg = _shorten(str(e), 1200)
                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result="ERROR",
                    message=f"item_extract: lxml parse failed: {msg}",
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="ERROR",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                err += 1
                continue

            # （任意）CDAじゃないっぽいなら SKIP へ
            if not _is_cda_clinical_document(root):
                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result="SKIP",
                    message="item_extract: not CDA ClinicalDocument",
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="SKIP",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                continue

            try:
                rows_out = _collect_observations_as_raw_items(tree, item_master_map=item_master_map)
                written = 0

                for row in rows_out:
                    namecode = str(row["namecode"])
                    occurrence_no = int(row["occurrence_no"])

                    db_upsert_xml_item_value(
                        cur_medi,
                        xml_sha256=xml_sha,
                        zip_sha256=zip_sha,
                        zip_inner_path=inner,
                        zip_inner_path_sha256=inner_sha,
                        namecode=namecode,
                        occurrence_no=occurrence_no,
                        value_raw=row.get("value_raw"),
                        value_type=row.get("value_type"),
                        unit=row.get("unit"),
                        code_system=row.get("code_system"),
                        code_value=row.get("code_value"),
                        code_display=row.get("code_display"),
                        extracted_run_id=run_id,
                    )
                    written += 1

                if written > 0:
                    result = "OK"
                    receipt_status = "OK"
                    ok += 1
                else:
                    result = "ERROR"
                    receipt_status = "ERROR"
                    zero_hit += 1

                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result=result,
                    message=f"item_extract: written={written}",
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status=receipt_status,
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )

            except Exception as e:
                msg = _shorten(str(e), 1500)
                db_insert_xml_process_log(
                    cur_medi,
                    run_id=run_id,
                    xml_sha256=xml_sha,
                    step="EXTRACT_ITEMS",
                    result="ERROR",
                    message=f"item_extract exception: {msg}",
                )
                db_update_items_extract_fields(
                    cur_medi,
                    xml_receipt_id=xml_receipt_id,
                    items_extract_status="ERROR",
                    items_extracted_run_id=run_id,
                    items_extracted_at_now=True,
                )
                err += 1
                continue

            if processed % 50 == 0:
                conn_medi.commit()

        conn_medi.commit()

        summary = (
            f"item_extract processed={processed} ok={ok} err={err} zero_hit={zero_hit} "
            f"limit={limit if limit > 0 else 'FULL'}"
        )
        db_finish_run(cur_medi, run_id, summary)
        conn_medi.commit()

        print(f"[ITEM_EXTRACT] {summary}")
        return 0 if err == 0 and zero_hit == 0 else 2

    finally:
        try:
            for zf in list(zip_cache.values()):
                try:
                    zf.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            conn_phr.close()
        except Exception:
            pass
        try:
            conn_medi.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
