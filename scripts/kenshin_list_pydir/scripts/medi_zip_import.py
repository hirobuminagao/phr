# -*- coding: utf-8 -*-
"""
scripts/medi_zip_import.py

MODE:
- ZIP_IMPORT : ZIP受領台帳 + (optional) XML棚卸し
- XML_EXTRACT: medi_xml_receipts の PENDING を対象に、well-formed + document_id抽出して更新
- FULL       : ZIP_IMPORT -> XML_EXTRACT

env:
  MEDI_IMPORT_MODE=ZIP_IMPORT|XML_EXTRACT|FULL (default ZIP_IMPORT)

ZIP_IMPORT:
  MEDI_IMPORT_XML_ENABLED=true/false
  MEDI_IMPORT_XML_PARSE_WELLFORMED=true/false  # 棚卸し時の軽いチェック（任意）
    - NOTE: well-formed OKでも status は PENDING のまま（EXTRACTへ回す）
    - 失敗時のみ status=ERROR にする

XML_EXTRACT:
  MEDI_IMPORT_XML_EXTRACT_LIMIT=500
  MEDI_IMPORT_XML_TARGET_STATUS=PENDING

DB (必須):
  MEDI_IMPORT_DB_HOST
  MEDI_IMPORT_DB_PORT
  MEDI_IMPORT_DB_NAME
  MEDI_IMPORT_DB_USER
  MEDI_IMPORT_DB_PASSWORD

PATH:
  MEDI_IMPORT_INPUT_ROOT=medi_input
  MEDI_IMPORT_TEMP_ROOT=medi_work/tmp_unzip
"""

from __future__ import annotations

# --- path bootstrap (MUST be before importing kenshin_lib) ---
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # = kenshin_list_pydir
sys.path.insert(0, str(BASE_DIR))
# ------------------------------------------------------------

import os
import logging
import hashlib
import shutil
from datetime import datetime
from typing import Optional, Tuple, Iterable

from dotenv import load_dotenv
import xml.etree.ElementTree as ET

import mysql.connector
from mysql.connector.cursor import MySQLCursorDict

from kenshin_lib.medi.db_medi import (
    db_insert_run,
    db_finish_run,
    db_get_zip_receipt_id_by_sha,
    db_upsert_zip_receipt,
    db_insert_zip_receipt_run,
    db_get_xml_receipt_id_by_sha,
    db_upsert_xml_receipt,
    db_insert_xml_receipt_run,
)

from kenshin_lib.medi.xml_extract import xml_extract_phase

from kenshin_lib.medi.zip_passwords import get_password_candidates
from kenshin_lib.medi.zip_extract import extract_zip_to_temp as lib_extract_zip_to_temp


# -----------------------------
# Logging / env utils
# -----------------------------
def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    import time as _time
    from datetime import datetime
    from zoneinfo import ZoneInfo

    JST = ZoneInfo("Asia/Tokyo")

    class JSTFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=JST)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]  # ,mmm

    logger = logging.getLogger("medi_zip_import")
    logger.setLevel(getattr(logging, level, logging.INFO))

    handler = logging.StreamHandler()
    handler.setFormatter(JSTFormatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger



def env_required(key: str) -> str:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        raise RuntimeError(f"必須環境変数 {key} が設定されていません")
    return v.strip()


def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


# -----------------------------
# DB connect helpers (MEDI_IMPORT_DB_* を正とする)
# -----------------------------
def load_medi_db_params() -> dict:
    host = env_required("MEDI_IMPORT_DB_HOST")
    port = int(env_required("MEDI_IMPORT_DB_PORT"))
    name = env_required("MEDI_IMPORT_DB_NAME")
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


def connect_medi(params: dict):
    conn = mysql.connector.connect(**params)
    return conn


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


# -----------------------------
# file utils
# -----------------------------
def parse_facility_folder_name(folder_name: str) -> Tuple[str, str]:
    if "_" in folder_name:
        code, name = folder_name.split("_", 1)
        return code.strip(), name.strip()
    return folder_name.strip(), ""


def list_facility_dirs(medi_input_root: Path) -> list[Path]:
    if not medi_input_root.exists():
        raise RuntimeError(f"MEDI_IMPORT_INPUT_ROOT が存在しません: {medi_input_root}")
    if not medi_input_root.is_dir():
        raise RuntimeError(f"MEDI_IMPORT_INPUT_ROOT はディレクトリではありません: {medi_input_root}")
    return [p for p in sorted(medi_input_root.iterdir()) if p.is_dir() and not p.name.startswith(".")]


def iter_zip_files(facility_dir: Path) -> Iterable[Path]:
    for p in sorted(facility_dir.iterdir()):
        if p.is_file() and p.suffix.lower() == ".zip":
            yield p


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def safe_rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def find_data_dirs(root: Path) -> list[Path]:
    data_dirs: list[Path] = []
    if not root.exists():
        return data_dirs
    for p in root.rglob("*"):
        if p.is_dir() and p.name == "DATA":
            data_dirs.append(p)
    data_dirs.sort(key=lambda x: (len(x.parts), str(x)))
    return data_dirs


def list_xml_files_anywhere(root: Path) -> list[Path]:
    if not root.exists():
        return []
    xs = [p for p in root.rglob("*.xml") if p.is_file()]
    xs.sort(key=lambda x: str(x))
    return xs


def list_xml_files_under_data_dirs(root: Path, data_dirs: list[Path]) -> list[Path]:
    xs: list[Path] = []
    for d in data_dirs:
        if d.exists() and d.is_dir():
            # DATA直下だけに限定したいなら iterdir() に変える
            xs.extend([p for p in d.rglob("*.xml") if p.is_file()])
    xs.sort(key=lambda x: str(x))
    return xs


def zip_has_any_file(root: Path) -> bool:
    if not root.exists():
        return False
    for p in root.rglob("*"):
        if p.is_file():
            return True
    return False


# -----------------------------
# ZIP_IMPORT: XML棚卸し（zip_temp_dir上の実ファイルからsha計算＆台帳）
# -----------------------------
def process_xmls_in_zip(
    logger: logging.Logger,
    cur,
    *,
    run_id: int,
    zip_sha256: str,
    zip_temp_dir: Path,
    facility_code: str,
    facility_name: str,
    do_wellformed_check: bool,
) -> Tuple[int, int, int, int]:
    """
    変更点:
    - DATAが1つでない場合もスキップしない
    - DATAが複数 → 全DATA配下のXMLを拾う
    - DATAなし → ZIP全体からXMLを拾う
    """
    data_dirs = find_data_dirs(zip_temp_dir)

    if len(data_dirs) >= 1:
        xml_files = list_xml_files_under_data_dirs(zip_temp_dir, data_dirs)
    else:
        xml_files = list_xml_files_anywhere(zip_temp_dir)

    xml_total = xml_new = xml_seen = xml_error = 0

    for xf in xml_files:
        xml_total += 1

        zip_inner_path = str(xf.relative_to(zip_temp_dir)).replace("\\", "/")
        zip_inner_path_sha256 = sha256_text(zip_inner_path)

        xml_sha = ""
        try:
            xml_sha = sha256_file(xf)
            existed_id = db_get_xml_receipt_id_by_sha(cur, xml_sha)
            action = "NEW" if existed_id is None else "SEEN"
            if action == "NEW":
                xml_new += 1
            else:
                xml_seen += 1

            st = xf.stat()
            file_size = int(st.st_size)
            file_mtime = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S.%f")

            status = "PENDING"
            error_code = None
            error_message = None

            if do_wellformed_check:
                try:
                    ET.parse(str(xf))
                except Exception as e:
                    status = "ERROR"
                    error_code = "XML_PARSE"
                    error_message = str(e)[:1000]
                    xml_error += 1

            db_upsert_xml_receipt(
                cur,
                run_id=run_id,
                zip_sha256=zip_sha256,
                zip_inner_path=zip_inner_path,
                zip_inner_path_sha256=zip_inner_path_sha256,
                xml_sha256=xml_sha,
                file_size=file_size,
                file_mtime=file_mtime,
                status=status,
                error_code=error_code,
                error_message=error_message,
                facility_code=facility_code,
                facility_name=facility_name,
            )

            msg = None
            if status == "ERROR":
                msg = f"{error_code}:{error_message}" if error_code else (error_message or "error")
            db_insert_xml_receipt_run(cur, run_id=run_id, xml_sha256=xml_sha, action=action, message=msg)

        except Exception as e:
            xml_error += 1
            logger.exception(f"  [XML][ERROR] {xf.name!r} unexpected: {e}")
            if not xml_sha:
                xml_sha = "0" * 64

            db_upsert_xml_receipt(
                cur,
                run_id=run_id,
                zip_sha256=zip_sha256,
                zip_inner_path=zip_inner_path,
                zip_inner_path_sha256=zip_inner_path_sha256,
                xml_sha256=xml_sha,
                file_size=None,
                file_mtime=None,
                status="ERROR",
                error_code="XML_IMPORT",
                error_message=f"unexpected error: {str(e)}",
                facility_code=facility_code,
                facility_name=facility_name,
            )
            db_insert_xml_receipt_run(cur, run_id=run_id, xml_sha256=xml_sha, action="SEEN", message=f"XML_IMPORT:{str(e)}")

    return (xml_total, xml_new, xml_seen, xml_error)


# -----------------------------
# ZIP_IMPORT phase
# -----------------------------
def zip_import_phase(
    logger: logging.Logger,
    cur,
    conn,
    *,
    run_id: int,
    medi_input_root: Path,
    temp_root: Path,
    xml_enabled: bool,
    xml_wellformed: bool,
) -> dict:
    logger.info(f"XML_ENABLED={xml_enabled} XML_WELLFORMED_CHECK={xml_wellformed}")

    facility_count = zip_found = zip_new = zip_seen = zip_ok = zip_error = zip_skipped = 0
    xml_total_all = xml_new_all = xml_seen_all = xml_error_all = xml_skipped_all = 0

    run_temp_base = temp_root / f"run_{run_id:06d}"
    ensure_dir(run_temp_base)

    facility_dirs = list_facility_dirs(medi_input_root)
    facility_count = len(facility_dirs)
    logger.info(f"Facility folders found: {facility_count}")

    for facility_dir in facility_dirs:
        facility_folder_name = facility_dir.name
        facility_code, facility_name = parse_facility_folder_name(facility_folder_name)

        zips = list(iter_zip_files(facility_dir))
        logger.info(f"[FACILITY] {facility_folder_name!r} zip_count={len(zips)}")
        if not zips:
            zip_skipped += 1
            continue

        for zip_path in zips:
            zip_found += 1
            zip_name = zip_path.name
            zip_abs = str(zip_path.resolve())

            try:
                zip_sha = sha256_file(zip_path)
            except Exception as e:
                zip_error += 1
                logger.exception(f"  [ERROR] {zip_name!r} sha256 failed: {e}")
                continue

            logger.info(f"  [ZIP] {zip_name!r} sha256={zip_sha}")

            existed_id = db_get_zip_receipt_id_by_sha(cur, zip_sha)
            action = "NEW" if existed_id is None else "SEEN"
            if action == "NEW":
                zip_new += 1
            else:
                zip_seen += 1

            zip_temp_dir = run_temp_base / zip_sha

            structure_status = "ERROR"
            error_code: Optional[str] = None
            error_message: Optional[str] = None
            messages: list[str] = []
            data_dir_count: Optional[int] = None
            data_xml_count: Optional[int] = None

            # 1) ZIP展開（パスワード候補取得→試行）
            res_ok = False
            try:
                pwd_candidates = get_password_candidates(
                    cur,
                    facility_code=facility_code,
                    facility_folder_name=facility_folder_name,
                    zip_name=zip_name,
                    zip_sha256=zip_sha,
                )

                ext_res = lib_extract_zip_to_temp(
                    zip_path,
                    zip_temp_dir,
                    pwd_candidates=pwd_candidates,
                )

                if not ext_res.ok:
                    structure_status = "ERROR"
                    error_code = ext_res.error_code or "ZIP_UNEXPECTED"
                    error_message = (ext_res.message or "")[:2000] or None
                    messages.append(f"ZIP展開に失敗: {error_code}")
                    if error_message:
                        messages.append(error_message)
                    res_ok = False
                else:
                    res_ok = True

            except Exception as e:
                structure_status = "ERROR"
                error_code = "ZIP_UNEXPECTED"
                error_message = str(e)[:2000]
                messages.append("ZIP展開に失敗: ZIP_UNEXPECTED")
                messages.append(error_message)
                logger.exception(f"  [ERROR] {zip_name!r} unexpected during extract: {e}")
                res_ok = False

            # 2) 展開できた場合：構造判定を “XMLが拾えるか” に変更
            if res_ok:
                try:
                    # 2-1) 空ZIP（展開後にファイルが存在しない）
                    if not zip_has_any_file(zip_temp_dir):
                        structure_status = "ERROR"
                        error_code = "ZIP_EMPTY_CONTENT"
                        messages.append("ZIP展開後にファイルが存在しません（空ZIP/0バイトの可能性）")
                        data_dir_count = 0
                        data_xml_count = 0
                    else:
                        data_dirs = find_data_dirs(zip_temp_dir)
                        data_dir_count = len(data_dirs)

                        if data_dir_count >= 1:
                            xml_files = list_xml_files_under_data_dirs(zip_temp_dir, data_dirs)
                            # “DATA複数” は異常として記録しつつ拾う
                            if data_dir_count >= 2:
                                error_code = "STRUCT_MULTI_DATA_DIR"
                                sample = [str(d.relative_to(zip_temp_dir)) for d in data_dirs[:5]]
                                messages.append(f"DATAフォルダが複数検出されました: count={data_dir_count}")
                                messages.append("DATA候補(先頭5): " + ", ".join(sample))
                        else:
                            # DATAなしだが拾えるXMLがあるかチェック（ZIP全体）
                            xml_files = list_xml_files_anywhere(zip_temp_dir)
                            error_code = "STRUCT_NO_DATA_DIR"
                            messages.append("DATAフォルダが検出できません（ただしXMLを探索して棚卸し対象にします）")

                        data_xml_count = len(xml_files)

                        if data_xml_count > 0:
                            structure_status = "OK"
                            # “普通じゃないけど拾えた” は error_code + message に残す
                            if data_dir_count == 1 and error_code in (None, ""):
                                pass
                        else:
                            structure_status = "ERROR"
                            # DATAがあって0件は従来どおり
                            if data_dir_count == 1:
                                error_code = "STRUCT_ZERO_XML"
                                messages.append("DATA配下にXMLが0件です（空）")
                            else:
                                error_code = error_code or "STRUCT_ZERO_XML"
                                messages.append("ZIP内にXMLが検出できません")

                except Exception as e:
                    structure_status = "ERROR"
                    error_code = "ZIP_UNEXPECTED"
                    error_message = str(e)[:2000]
                    messages.append("構造判定中に例外: ZIP_UNEXPECTED")
                    messages.append(error_message)
                    logger.exception(f"  [ERROR] {zip_name!r} unexpected during structure check: {e}")

            # 3) DBへ必ず記帳（OKでもERRORでも）
            try:
                structure_message = " | ".join(messages) if messages else None

                zip_receipt_id = db_upsert_zip_receipt(
                    cur,
                    run_id=run_id,
                    facility_folder_name=facility_folder_name,
                    facility_code=facility_code,
                    facility_name=facility_name,
                    zip_name=zip_name,
                    zip_path=zip_abs,
                    zip_sha256=zip_sha,
                    structure_status=structure_status,
                    structure_message=structure_message,
                    data_dir_count=data_dir_count,
                    data_xml_count=data_xml_count,
                    error_code=error_code,
                    error_message=error_message,
                )

                db_insert_zip_receipt_run(
                    cur,
                    run_id=run_id,
                    zip_receipt_id=zip_receipt_id,
                    zip_sha256=zip_sha,
                    action=action,
                )

                conn.commit()

            except Exception as e:
                zip_error += 1
                logger.exception(f"  [ERROR] {zip_name!r} DB upsert failed: {e}")
                conn.rollback()
                try:
                    safe_rmtree(zip_temp_dir)
                except Exception:
                    pass
                continue

            # 4) counters
            if structure_status == "OK":
                zip_ok += 1
            else:
                zip_error += 1

            logger.info(
                f"  [STRUCT] status={structure_status} error_code={error_code} "
                f"data_dir_count={data_dir_count} data_xml_count={data_xml_count}"
            )

            # 5) XML棚卸し（OKのみ = XMLが拾える見込みがある）
            if xml_enabled and structure_status == "OK":
                try:
                    t, n, s, e = process_xmls_in_zip(
                        logger,
                        cur,
                        run_id=run_id,
                        zip_sha256=zip_sha,
                        zip_temp_dir=zip_temp_dir,
                        facility_code=facility_code,
                        facility_name=facility_name,
                        do_wellformed_check=xml_wellformed,
                    )
                    conn.commit()
                    xml_total_all += t
                    xml_new_all += n
                    xml_seen_all += s
                    xml_error_all += e
                    logger.info(f"  [XML] total={t} new={n} seen={s} error={e}")
                except Exception as e:
                    logger.exception(f"  [XML][ERROR] zip_sha={zip_sha} unexpected: {e}")
                    conn.rollback()
            elif xml_enabled and structure_status != "OK":
                xml_skipped_all += 1

            # 6) cleanup
            try:
                safe_rmtree(zip_temp_dir)
            except Exception:
                pass

    return {
        "facility": facility_count,
        "zips_found": zip_found,
        "zip_new": zip_new,
        "zip_seen": zip_seen,
        "zip_ok": zip_ok,
        "zip_error": zip_error,
        "zip_skipped": zip_skipped,
        "xml_total": xml_total_all,
        "xml_new": xml_new_all,
        "xml_seen": xml_seen_all,
        "xml_error": xml_error_all,
        "xml_skipped_zip": xml_skipped_all,
        "xml_enabled": xml_enabled,
    }


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    logger = setup_logger()

    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f".env loaded: {env_path}")
    else:
        logger.warning(f".env not found: {env_path}（環境変数を直接使用）")

    mode = os.getenv("MEDI_IMPORT_MODE", "ZIP_IMPORT").strip().upper()

    input_root_raw = env_required("MEDI_IMPORT_INPUT_ROOT")
    medi_input_root = (BASE_DIR / input_root_raw).resolve() if not Path(input_root_raw).is_absolute() else Path(input_root_raw)

    temp_root_raw = os.getenv("MEDI_IMPORT_TEMP_ROOT", "medi_work/tmp_unzip").strip()
    temp_root = (BASE_DIR / temp_root_raw).resolve() if not Path(temp_root_raw).is_absolute() else Path(temp_root_raw)

    note_prefix = os.getenv("MEDI_IMPORT_NOTE", "").strip() or None

    params = load_medi_db_params()

    logger.info(f"MODE={mode}")
    logger.info(f"BASE_DIR={BASE_DIR}")
    logger.info(f"MEDI_INPUT_ROOT={medi_input_root}")
    logger.info(f"TEMP_ROOT={temp_root}")
    logger.info(f"DB={params['host']}:{params['port']}/{params['database']} user={params['user']}")

    conn = connect_medi(params)
    try:
        cur = dict_cursor(conn)

        run_id = db_insert_run(cur, str(medi_input_root), note_prefix)
        conn.commit()
        logger.info(f"Run started: run_id={run_id}")

        xml_enabled = env_bool("MEDI_IMPORT_XML_ENABLED", False)
        xml_wellformed = env_bool("MEDI_IMPORT_XML_PARSE_WELLFORMED", False)

        limit = env_int("MEDI_IMPORT_XML_EXTRACT_LIMIT", 500)
        target_status = os.getenv("MEDI_IMPORT_XML_TARGET_STATUS", "PENDING").strip().upper()

        if mode == "XML_EXTRACT":
            processed, ok, err = xml_extract_phase(logger, cur, run_id=run_id, target_status=target_status, limit=limit)
            conn.commit()

            summary = f"xml_extract processed={processed} ok={ok} error={err} target_status={target_status} limit={limit}"
            final_note = summary if not note_prefix else f"{note_prefix} | {summary}"
            db_finish_run(cur, run_id, final_note)
            conn.commit()
            logger.info(f"Run finished: run_id={run_id} {summary}")
            return 0

        if mode == "ZIP_IMPORT":
            s = zip_import_phase(
                logger,
                cur,
                conn,
                run_id=run_id,
                medi_input_root=medi_input_root,
                temp_root=temp_root,
                xml_enabled=xml_enabled,
                xml_wellformed=xml_wellformed,
            )

            summary_zip = (
                f"facility={s['facility']}, zips_found={s['zips_found']}, new={s['zip_new']}, seen={s['zip_seen']}, "
                f"ok={s['zip_ok']}, error={s['zip_error']}, skipped={s['zip_skipped']}"
            )
            if s["xml_enabled"]:
                summary_xml = (
                    f"xml_total={s['xml_total']}, new={s['xml_new']}, seen={s['xml_seen']}, "
                    f"error={s['xml_error']}, xml_skipped_zip={s['xml_skipped_zip']}"
                )
                summary = summary_zip + " | " + summary_xml
            else:
                summary = summary_zip

            final_note = summary if not note_prefix else f"{note_prefix} | {summary}"
            db_finish_run(cur, run_id, final_note)
            conn.commit()
            logger.info(f"Run finished: run_id={run_id} {summary}")
            return 0

        if mode == "FULL":
            s = zip_import_phase(
                logger,
                cur,
                conn,
                run_id=run_id,
                medi_input_root=medi_input_root,
                temp_root=temp_root,
                xml_enabled=xml_enabled,
                xml_wellformed=xml_wellformed,
            )

            processed, ok, err = xml_extract_phase(logger, cur, run_id=run_id, target_status=target_status, limit=limit)
            conn.commit()

            summary_zip = (
                f"facility={s['facility']}, zips_found={s['zips_found']}, new={s['zip_new']}, seen={s['zip_seen']}, "
                f"ok={s['zip_ok']}, error={s['zip_error']}, skipped={s['zip_skipped']}"
            )
            if s["xml_enabled"]:
                summary_xml = (
                    f"xml_total={s['xml_total']}, new={s['xml_new']}, seen={s['xml_seen']}, "
                    f"error={s['xml_error']}, xml_skipped_zip={s['xml_skipped_zip']}"
                )
                part_zip = summary_zip + " | " + summary_xml
            else:
                part_zip = summary_zip

            part_extract = f"xml_extract processed={processed} ok={ok} error={err} target_status={target_status} limit={limit}"
            summary = f"{part_zip} | {part_extract}"

            final_note = summary if not note_prefix else f"{note_prefix} | {summary}"
            db_finish_run(cur, run_id, final_note)
            conn.commit()
            logger.info(f"Run finished: run_id={run_id} {summary}")
            return 0

        summary = f"unknown mode: {mode} (expected ZIP_IMPORT/XML_EXTRACT/FULL)"
        final_note = summary if not note_prefix else f"{note_prefix} | {summary}"
        db_finish_run(cur, run_id, final_note)
        conn.commit()
        logger.error(f"Run finished: run_id={run_id} {summary}")
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
