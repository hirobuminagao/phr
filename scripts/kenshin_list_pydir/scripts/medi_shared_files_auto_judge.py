# -*- coding: utf-8 -*-
"""
scripts/medi_shared_files_auto_judge.py

medi_shared_files の NEW(=only_stage) の zip を対象に judge を行う。

確定フロー（このスクリプトだけで完結）:
- 対象: stage_status='NEW' AND ext='zip' AND sha256あり AND manual_judgement IS NULL
- zip内にxmlがあるか判定（zip_inspect.probe_zip_has_xml）
  - zip_has_xml が NULL の行だけ probe する（既に値があれば再計算しない）
  - 結果をDBへ反映: zip_has_xml, zip_xml_count, zip_xml_checked_at, note
- 判定:
  - zip_has_xml==1 → auto_judgement='KENSHIN'
  - zip_has_xml!=1 → auto_judgement='UNKNOWN'（確定的に非健診と言えないため）

env:
  (DB)  medi_zip_import.py と同じ MEDI_IMPORT_DB_* （必須）

  # judge対象
  MEDI_SHARED_AUTO_LIMIT=500     (任意: 0=無制限)
  MEDI_SHARED_AUTO_ONLY_STAGE=NEW (任意)

  # zip内xml判定
  MEDI_SHARED_AUTO_PROBE_ALWAYS=false (任意: trueならzip_has_xmlが埋まってても再probeして上書き)
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
from datetime import datetime
from typing import Any, Optional

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.cursor import MySQLCursorDict

from kenshin_lib.medi.db_shared_files import (
    db_select_new_zip_files_for_judge,
    db_update_zip_xml_probe,
    db_update_auto_judgement,
)

# ★正式採用: zip_inspect
from kenshin_lib.medi.zip_inspect import probe_zip_has_xml


# -----------------------------
# logging / env
# -----------------------------
def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    from zoneinfo import ZoneInfo
    JST = ZoneInfo("Asia/Tokyo")

    class JSTFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=JST)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    logger = logging.getLogger("medi_shared_files_auto_judge")
    logger.setLevel(getattr(logging, level, logging.INFO))

    h = logging.StreamHandler()
    h.setFormatter(JSTFormatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.handlers.clear()
    logger.addHandler(h)
    logger.propagate = False
    return logger


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
        # 文字化け/bytes化の予防（会社側環境対策）
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
    }


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


def as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)


def parse_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = as_str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None


def main() -> None:
    env_path = BASE_DIR / ".env"
    load_dotenv(env_path)

    logger = setup_logger()
    logger.info(f"dotenv = {env_path}")

    limit = env_int("MEDI_SHARED_AUTO_LIMIT", 500)
    only_stage = os.getenv("MEDI_SHARED_AUTO_ONLY_STAGE", "NEW").strip() or "NEW"
    probe_always = env_bool("MEDI_SHARED_AUTO_PROBE_ALWAYS", False)

    conn = mysql.connector.connect(**load_medi_db_params())
    cur = dict_cursor(conn)

    try:
        rows = db_select_new_zip_files_for_judge(cur, limit=limit, only_stage=only_stage)
        logger.info(f"target rows={len(rows)} only_stage={only_stage} limit={limit if limit else 'NO LIMIT'} probe_always={probe_always}")

        changed = 0
        probed = 0
        kenshin = 0
        unknown = 0
        probe_failed = 0

        for r in rows:
            sid = int(as_str(r.get("shared_file_id")) or "0")
            if sid <= 0:
                continue

            zip_path = Path(as_str(r.get("path")))
            has_xml_db = parse_int_or_none(r.get("zip_has_xml"))
            xml_count_db = parse_int_or_none(r.get("zip_xml_count"))

            # --- zip内xml判定 ---
            need_probe = probe_always or (has_xml_db is None)
            if need_probe:
                pr = probe_zip_has_xml(zip_path)
                # pr: ZipXmlProbeResult(ok, has_xml, xml_count, note)
                # DBへ反映（checked_atもここで入る）
                if pr.ok:
                    db_update_zip_xml_probe(
                        cur,
                        shared_file_id=sid,
                        zip_has_xml=1 if pr.has_xml else 0,
                        zip_xml_count=int(pr.xml_count),
                        note=pr.note,
                    )
                    has_xml_db = 1 if pr.has_xml else 0
                    xml_count_db = int(pr.xml_count)
                else:
                    # 判定失敗もDBへメモ（zip_has_xmlはNULLのままでもいいが、ここでは0に倒さずNULL維持）
                    db_update_zip_xml_probe(
                        cur,
                        shared_file_id=sid,
                        zip_has_xml=None,
                        zip_xml_count=None,
                        note=pr.note or "zip xml probe failed",
                    )
                    probe_failed += 1
                    has_xml_db = None
                    xml_count_db = None
                probed += 1

            # --- auto_judgement決定（シンプル固定） ---
            if has_xml_db == 1:
                auto = "KENSHIN"
                note = f"auto:KENSHIN (zip_has_xml=1 xml_count={xml_count_db if xml_count_db is not None else '?'})"
                kenshin += 1
            else:
                auto = "UNKNOWN"
                if has_xml_db == 0:
                    note = "auto:UNKNOWN (zip_has_xml=0)"
                else:
                    note = "auto:UNKNOWN (zip_has_xml=NULL; probe failed or not available)"
                unknown += 1

            db_update_auto_judgement(cur, shared_file_id=sid, auto_judgement=auto, note=note)
            changed += 1

            if changed % 200 == 0:
                conn.commit()
                logger.info(f"progress changed={changed} probed={probed} kenshin={kenshin} unknown={unknown} probe_failed={probe_failed}")

        conn.commit()
        logger.info(f"DONE changed={changed} probed={probed} kenshin={kenshin} unknown={unknown} probe_failed={probe_failed}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
