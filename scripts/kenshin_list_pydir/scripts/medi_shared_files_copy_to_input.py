# -*- coding: utf-8 -*-
"""
scripts/medi_shared_files_copy_to_input.py

【目的】
共有フォルダ上で「健診対象として確定したZIP」を、取り込み用の input フォルダ（MEDI_IMPORT_INPUT_ROOT）へコピーする。

【設計方針】
- このスクリプトは「健診対象かどうか」の判定は行わない（判定済みの事実だけをDBから拾う）。
- ここではZIPの中身確認（probe / inspect）は行わない。
  ZIP内XML有無などの判定結果は、上流のスクリプトでDBへ記帳済みであること。
- フォルダの配置先は alias（src_folder_raw -> dst_folder_norm）に従う。

【前提条件（DBで満たされていること）】
- medi_shared_files.sha256 が入っている（hash済み）
- medi_shared_files.zip_has_xml = 1（probe済み）
- 対応する src_folder_raw に対して、medi_shared_folder_aliases.dst_folder_norm が確定している

【抽出ポリシー（SQL）】
- stage_status='NEW', ext='zip'
- COALESCE(manual_judgement, auto_judgement)='KENSHIN'
- alias が存在し、dst_folder_norm が空でない
- まだ取り込み済みではない（medi_zip_receipts.zip_sha256 に同一shaが存在しない）

【処理内容】
- コピー先: <MEDI_IMPORT_INPUT_ROOT>/<dst_folder_norm>/<file_name>
- コピー先に同名が存在し、overwrite=false の場合:
    stage_status=INPUT_COPIED として「コピー済み扱い」にする（再試行を抑止）
- コピー成功:
    stage_status=INPUT_COPIED
- 失敗:
    原則 stage_status=NEW のまま（リトライ可能）
    ただしソースが存在しない場合のみ stage_status=SKIPPED
- いずれも note に短い理由を残す

【環境変数】
  MEDI_IMPORT_INPUT_ROOT (必須)
  MEDI_SHARED_COPY_LIMIT=500
  MEDI_SHARED_COPY_OVERWRITE=false

【DB接続】
  MEDI_IMPORT_DB_HOST / PORT / NAME / USER / PASSWORD

【注意】
- ファイルI/Oはトランザクションにならないため、DB更新はベストエフォートで進捗を反映する。
- ログ時刻は運用者の視認性のため Asia/Tokyo（JST）で出力する。
"""

from __future__ import annotations

# --- path bootstrap (IMPORTより先に必要) ---
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # kenshin_list_pydir
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
# -------------------------------------------

import os
import shutil
import logging
from typing import Any, Optional, Mapping, cast

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.cursor import MySQLCursorDict

from kenshin_lib.medi.db_shared_files import db_mark_stage_status



# -----------------------------
# Logger / env
# -----------------------------
def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    from datetime import datetime
    from zoneinfo import ZoneInfo

    JST = ZoneInfo("Asia/Tokyo")

    class JSTFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=JST)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    logger = logging.getLogger("medi_shared_files_copy_to_input")
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
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
    }


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


# -----------------------------
# bytes/Row safety
# -----------------------------
RowLike = Mapping[Any, Any]


def as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)


def row_to_strkey_dict(r: Any) -> dict[str, Any]:
    if r is None:
        return {}
    m = cast(RowLike, r)
    out: dict[str, Any] = {}
    for k, v in m.items():
        kk = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
        out[kk] = v
    return out


def parse_int_or_none(v: Any) -> Optional[int]:
    s = as_str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None


# -----------------------------
# SQL（事実ベース抽出のみ）
# -----------------------------
SQL_SELECT_TARGETS = """
SELECT
  sf.shared_file_id,
  sf.path,
  sf.file_name,
  sf.sha256,
  sf.src_folder_raw,
  a.dst_folder_norm AS dst_folder_norm
FROM medi_shared_files sf
JOIN medi_shared_folder_aliases a
  ON a.is_active=1
 AND a.src_folder_raw = sf.src_folder_raw
LEFT JOIN medi_zip_receipts zr
  ON zr.zip_sha256 = sf.sha256
WHERE sf.stage_status='NEW'
  AND sf.ext='zip'
  AND sf.sha256 IS NOT NULL AND sf.sha256 <> ''
  AND COALESCE(sf.manual_judgement, sf.auto_judgement)='KENSHIN'
  AND sf.zip_has_xml=1
  AND a.dst_folder_norm IS NOT NULL AND a.dst_folder_norm <> ''
  AND zr.zip_receipt_id IS NULL
ORDER BY sf.first_seen_at ASC
LIMIT %s
"""


def main() -> None:

    env_path = BASE_DIR / ".env"
    load_dotenv(env_path)

    logger = setup_logger()
    logger.info(f"dotenv = {env_path}")

    input_root = Path(env_required("MEDI_IMPORT_INPUT_ROOT"))
    input_root.mkdir(parents=True, exist_ok=True)

    limit = env_int("MEDI_SHARED_COPY_LIMIT", 500)
    overwrite = env_bool("MEDI_SHARED_COPY_OVERWRITE", False)

    conn = mysql.connector.connect(**load_medi_db_params())
    cur = dict_cursor(conn)

    copied = 0
    skipped = 0
    failed = 0

    try:
        cur.execute(SQL_SELECT_TARGETS, (int(limit),))
        raw_rows = cur.fetchall() or []
        rows = [row_to_strkey_dict(rr) for rr in raw_rows if rr is not None]
        logger.info(f"target rows={len(rows)} limit={limit} overwrite={overwrite}")

        for r in rows:
            sid = parse_int_or_none(r.get("shared_file_id")) or 0
            if sid <= 0:
                continue

            src_path = Path(as_str(r.get("path")))
            file_name = as_str(r.get("file_name")).strip()
            dst_folder = as_str(r.get("dst_folder_norm")).strip()

            if not dst_folder:
                db_mark_stage_status(cur, shared_file_id=sid, stage_status="NEW", note="skip: alias dst_folder_norm is empty")
                skipped += 1
                continue

            if not file_name:
                db_mark_stage_status(cur, shared_file_id=sid, stage_status="NEW", note="fail: file_name is empty in DB")
                failed += 1
                continue

            # ここまで来たら「DB的にはコピー可能」だが、現物が無い等は起こりうる
            try:
                if not src_path.exists():
                    db_mark_stage_status(cur, shared_file_id=sid, stage_status="SKIPPED", note="skip: source missing")
                    skipped += 1
                    continue
            except Exception as e:
                db_mark_stage_status(cur, shared_file_id=sid, stage_status="NEW", note=f"fail: exists check error: {e}")
                failed += 1
                continue

            dst_dir = input_root / dst_folder
            try:
                dst_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                db_mark_stage_status(cur, shared_file_id=sid, stage_status="NEW", note=f"fail: mkdir error: {dst_dir} {e}")
                failed += 1
                continue

            # ★ここ重要：DBの file_name を正にして input 側の配置名を決める
            dst_path = dst_dir / file_name

            if dst_path.exists() and not overwrite:
                # “コピー済み扱い”で閉じる（次回以降も無駄にトライしない）
                db_mark_stage_status(
                    cur,
                    shared_file_id=sid,
                    stage_status="INPUT_COPIED",
                    note=f"skip: already exists in input (no overwrite) dst={dst_path} sha256={as_str(r.get('sha256'))}",
                )
                skipped += 1
                continue

            try:
                shutil.copy2(str(src_path), str(dst_path))
                db_mark_stage_status(
                    cur,
                    shared_file_id=sid,
                    stage_status="INPUT_COPIED",
                    note=f"copied to {dst_folder} sha256={as_str(r.get('sha256'))}",
                )
                copied += 1
            except Exception as e:
                db_mark_stage_status(
                    cur,
                    shared_file_id=sid,
                    stage_status="NEW",
                    note=f"fail: copy error: {e} sha256={as_str(r.get('sha256'))}",
                )
                logger.warning(f"copy failed: {src_path} -> {dst_path} err={e}")
                failed += 1

            if (copied + skipped + failed) % 100 == 0 and (copied + skipped + failed) > 0:
                conn.commit()
                logger.info(f"progress copied={copied} skipped={skipped} failed={failed}")

        conn.commit()
        logger.info(f"DONE copied={copied} skipped={skipped} failed={failed}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
