# -*- coding: utf-8 -*-
"""
scripts/medi_shared_files_scan.py

共有フォルダ（UNC でもローカルでも可）をスキャンし、
medi_shared_files に観測結果をUPSERTする。

重要（今回の地雷対策）:
- “解凍済みフォルダ地獄” を踏むとUNC走査が死ぬので、探索パターンを基本 zip限定にする。
  -> shared_root.rglob("*.zip") をデフォルトに採用（探索対象そのものを絞る）

運用:
- 自動化しない。ひろが必要なときに手でキックする。
- 手動判定(manual_judgement) が入っているものは運用上の正とする（ここでは上書きしない）
- 自動判定はここでは UNKNOWN 固定（判定は別スクリプト/手動でOK）

env:
  MEDI_SHARED_ROOT=\\\\fs03\\...\\健診結果_請求   (必須)

  # zipだけにする場合（推奨）
  MEDI_SHARED_SCAN_EXTS=zip
  # 互換キー（ひろが先に書いてたやつ）も受ける
  MEDI_SHARED_EXTS=zip

  MEDI_SHARED_SCAN_LIMIT=0 (0=無制限 / >0なら件数制限)
  MEDI_SHARED_FACILITY_HINT_DEPTH=2 (親フォルダ何階層をヒントにするか)

DB:
  medi_zip_import.py と同じ MEDI_IMPORT_DB_* を使用
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
from typing import Optional, Iterable, Set

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.cursor import MySQLCursorDict

from kenshin_lib.medi.db_shared_files import (
    SharedFileRow,
    db_upsert_shared_file,
)

# -----------------------------
# Logging / env utils（zip_importと同系統）
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

    logger = logging.getLogger("medi_shared_files_scan")
    logger.setLevel(getattr(logging, level, logging.INFO))

    handler = logging.StreamHandler()
    handler.setFormatter(JSTFormatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


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


def norm_ext(p: Path) -> str:
    return p.suffix.lower().lstrip(".")


def parse_allow_exts() -> Set[str]:
    """
    優先順位:
      MEDI_SHARED_SCAN_EXTS > MEDI_SHARED_EXTS > default
    値は "zip,pdf,..." のカンマ区切り
    """
    exts = (
        os.getenv("MEDI_SHARED_SCAN_EXTS")
        or os.getenv("MEDI_SHARED_EXTS")
        or "zip,pdf,xlsx,xls,xml"
    ).strip()

    allow = {e.strip().lower() for e in exts.split(",") if e.strip()}
    return allow


def pick_facility_hint(p: Path, depth: int) -> str:
    """
    例: depth=2 なら「親/親」までをヒント文字列にする
    """
    parts = []
    cur = p.parent
    for _ in range(max(0, depth)):
        parts.append(cur.name)
        cur = cur.parent
        if cur == cur.parent:
            break
    parts = [x for x in parts if x]
    return "/".join(reversed(parts)) if parts else ""


def iter_targets(shared_root: Path, allow_exts: Set[str]) -> Iterable[Path]:
    """
    UNCの“解凍地獄”を踏まないため、探索パターンをできるだけ絞る。

    - allow_exts が {'zip'} のように 1種なら rglob("*.zip") にして探索自体を軽くする
    - 複数拡張子ならそれぞれを順に rglob("*.ext") で回す（rglob("*") は使わない）
    """
    if not allow_exts:
        # 何も指定されてないなら安全側に倒して zipのみ
        yield from shared_root.rglob("*.zip")
        return

    if len(allow_exts) == 1:
        ext = next(iter(allow_exts))
        yield from shared_root.rglob(f"*.{ext}")
        return

    for ext in sorted(allow_exts):
        yield from shared_root.rglob(f"*.{ext}")


def main() -> None:
    load_dotenv(BASE_DIR / ".env")
    logger = setup_logger()

    shared_root = Path(env_required("MEDI_SHARED_ROOT"))
    if not shared_root.exists():
        raise RuntimeError(f"MEDI_SHARED_ROOT が存在しません: {shared_root}")

    allow_exts = parse_allow_exts()
    scan_limit = env_int("MEDI_SHARED_SCAN_LIMIT", 0)
    hint_depth = env_int("MEDI_SHARED_FACILITY_HINT_DEPTH", 2)

    logger.info(f"scan root = {shared_root}")
    logger.info(f"allow exts = {sorted(allow_exts)}  limit={scan_limit if scan_limit else 'NO LIMIT'}")

    conn = mysql.connector.connect(**load_medi_db_params())
    cur = dict_cursor(conn)

    processed = 0
    upserted = 0
    ts = now_str()

    try:
        for fp in iter_targets(shared_root, allow_exts):
            # iter_targets は基本ファイルを返すが、念のためガード
            try:
                if fp.is_dir():
                    continue
            except Exception:
                continue

            processed += 1
            if scan_limit and processed > scan_limit:
                break

            # ext（念のため最終フィルタ）
            ext = norm_ext(fp)
            if allow_exts and ext not in allow_exts:
                continue

            # stat（UNCで失敗することがあるので握りつぶし）
            try:
                st = fp.stat()
                file_size = int(st.st_size)
                mtime = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S.%f")
            except Exception as e:
                logger.warning(f"stat failed: {fp} err={e}")
                file_size = 0
                mtime = None

            # src_folder_raw: shared_root直下のフォルダ名を生で取る（無ければNone）
            try:
                rel = fp.relative_to(shared_root)
                src_folder_raw = rel.parts[0] if len(rel.parts) >= 2 else None
            except Exception:
                src_folder_raw = None

            row = SharedFileRow(
                path=str(fp),
                src_folder_raw=src_folder_raw,
                dst_folder_norm=None,  # ここでは触らない（手動 or copy_to_input側で使う）
                facility_hint=pick_facility_hint(fp, hint_depth),
                file_name=fp.name,
                ext=ext,
                file_size=file_size,
                mtime=mtime,
                sha256=None,  # 重いのでここでは計算しない（必要なら別フェーズで）
                auto_judgement="UNKNOWN",
                manual_judgement=None,
                stage_status="NEW",
                note=None,
                first_seen_at=ts,
                last_seen_at=ts,
            )

            _shared_file_id = db_upsert_shared_file(cur, row)
            upserted += 1

            if processed % 2000 == 0:
                conn.commit()
                logger.info(f"progress: processed={processed} upserted={upserted}")

        conn.commit()
        logger.info(f"DONE: processed={processed} upserted={upserted}")

    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
