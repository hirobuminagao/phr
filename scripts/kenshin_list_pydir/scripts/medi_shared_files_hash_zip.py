# -*- coding: utf-8 -*-
"""
scripts/medi_shared_files_hash_zip.py

medi_shared_files のうち「zipで sha256 が未計算」の行だけを対象に、
ファイル内容SHA-256を計算して DB に埋める。

重要:
- 解凍しない（zipを開かない）
- ファイルを読み取るだけ（sha256計算）
- UNCでも動くが遅いので LIMIT を推奨

env:
  (DB)  medi_zip_import.py と同じ MEDI_IMPORT_DB_* を使用（必須）
  MEDI_SHARED_HASH_LIMIT=200          (任意: 0=無制限)
  MEDI_SHARED_HASH_ONLY_STAGE=NEW     (任意: 空なら全stage対象)
  MEDI_SHARED_HASH_CHUNK_MB=8         (任意)
"""

from __future__ import annotations

# --- path bootstrap ---
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))
# ----------------------

import os
import logging
import hashlib
from datetime import datetime
from typing import Optional, Any, Mapping, cast

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.cursor import MySQLCursorDict


# -----------------------------
# Pylance対策（Row型/bytes-key対策）
# -----------------------------
def row_to_strkey_dict(r: Any) -> dict[str, Any]:
    """
    mysql-connector の fetchall() 返り値の型スタブが環境によって曖昧で、
    - r が None の可能性
    - dictのキーが bytes の可能性
    を Pylance が強く疑ってくるため、ここで strキーdict に正規化する。
    """
    if r is None:
        return {}
    m = cast(Mapping[Any, Any], r)

    out: dict[str, Any] = {}
    for k, v in m.items():
        if isinstance(k, (bytes, bytearray)):
            kk = k.decode("utf-8", errors="ignore")
        else:
            kk = str(k)
        out[kk] = v
    return out


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

    logger = logging.getLogger("medi_shared_files_hash_zip")
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


def dict_cursor(conn) -> MySQLCursorDict:
    return conn.cursor(dictionary=True, buffered=True)


def sha256_file(path: Path, chunk_size: int) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def main() -> None:
    load_dotenv(BASE_DIR / ".env")
    logger = setup_logger()

    limit = env_int("MEDI_SHARED_HASH_LIMIT", 200)

    # 空文字なら全stage対象（フィルタ無し）
    only_stage_raw = os.getenv("MEDI_SHARED_HASH_ONLY_STAGE", "NEW")
    only_stage = (only_stage_raw or "").strip()

    chunk_mb = env_int("MEDI_SHARED_HASH_CHUNK_MB", 8)
    chunk_size = max(1024 * 1024, chunk_mb * 1024 * 1024)

    conn = mysql.connector.connect(**load_medi_db_params())
    cur = dict_cursor(conn)

    try:
        where_stage_sql = ""
        stage_param: Optional[str] = None
        if only_stage != "":
            where_stage_sql = "AND stage_status=%s"
            stage_param = only_stage

        # LIMIT無制限相当（MySQLの最大値）
        lim_param: int = int(limit) if (limit and limit > 0) else 18446744073709551615

        sql = f"""
        SELECT shared_file_id, path
        FROM medi_shared_files
        WHERE ext='zip'
          AND (sha256 IS NULL OR sha256='')
          {where_stage_sql}
        ORDER BY first_seen_at ASC
        LIMIT %s
        """

        if stage_param is not None:
            cur.execute(sql, (str(stage_param), int(lim_param)))
        else:
            cur.execute(sql, (int(lim_param),))

        raw_rows = cur.fetchall()
        rows = [row_to_strkey_dict(rr) for rr in raw_rows if rr is not None]

        logger.info(
            f"target zip rows={len(rows)} stage_filter={only_stage if only_stage else '(none)'} "
            f"limit={limit if limit else 'NO LIMIT'} chunk_mb={chunk_mb}"
        )

        done = 0
        missing = 0
        failed = 0
        processed = 0

        for r in rows:
            if not r:
                continue
            processed += 1

            sid = int(r.get("shared_file_id") or 0)
            if sid <= 0:
                continue

            p = Path(str(r.get("path") or ""))

            if not str(p) or not p.exists():
                missing += 1
                cur.execute(
                    """
                    UPDATE medi_shared_files
                    SET note=%s, updated_at=CURRENT_TIMESTAMP(6)
                    WHERE shared_file_id=%s
                    """,
                    ("source missing when hashing", sid),
                )
                if processed % 50 == 0:
                    conn.commit()
                    logger.info(f"progress done={done} missing={missing} failed={failed}")
                continue

            try:
                sha = sha256_file(p, chunk_size)
                cur.execute(
                    """
                    UPDATE medi_shared_files
                    SET sha256=%s, updated_at=CURRENT_TIMESTAMP(6)
                    WHERE shared_file_id=%s
                    """,
                    (sha, sid),
                )
                done += 1
            except Exception as e:
                failed += 1
                cur.execute(
                    """
                    UPDATE medi_shared_files
                    SET note=%s, updated_at=CURRENT_TIMESTAMP(6)
                    WHERE shared_file_id=%s
                    """,
                    (f"hash failed: {e}", sid),
                )

            if processed % 50 == 0:
                conn.commit()
                logger.info(f"progress done={done} missing={missing} failed={failed}")

        conn.commit()
        logger.info(f"DONE done={done} missing={missing} failed={failed}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
