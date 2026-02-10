# -*- coding: utf-8 -*-
"""
scripts/medi_shared_files_scan.py

【目的 / 役割】
共有フォルダ（UNC / ローカル）を走査し、
ファイル観測結果を medi_shared_files テーブルへ UPSERT する。
本スクリプトは「共有フォルダ観測フェーズ」の正本実装とする。

【責務の範囲（本スクリプトが行うこと）】
- 共有フォルダ配下を拡張子フィルタ付きで走査する
- ファイル単位で以下のメタ情報を取得・記帳する
  - path / file_name / ext / file_size / mtime
  - src_folder_raw（shared_root 直下の生フォルダ名）
  - facility_hint（親フォルダ階層からのヒント）
- medi_shared_files に path_hash を一意キーとして UPSERT する
- 観測時刻（first_seen_at / last_seen_at）を管理する

【明示的に行わないこと（責務外）】
- ZIP の中身を開く／XML を検査する
- sha256 の計算（重い処理のため別スクリプトに委譲）
- auto_judgement の判定ロジック
- input フォルダへのコピー
- stage_status の遷移管理（常に NEW を指定するのみ）

【UPSERT 契約（固定仕様）】
- 一意キー: path_hash = SHA1(path)
- first_seen_at:
    - 初回 INSERT 時のみセット
    - 既存行がある場合は更新しない
- last_seen_at:
    - 毎回の走査で必ず更新する
- sha256:
    - NULL で既存値を上書きしない（hash_zip フェーズ前提）
- manual_judgement:
    - 既存値がある場合は維持（運用上の正）
- auto_judgement:
    - 本スクリプトでは常に 'UNKNOWN' をセット
- stage_status:
    - 常に 'NEW' を指定（遷移は別フェーズで管理）

【探索ポリシー（安全性重視）】
- UNC 環境での過剰な再帰探索を避けるため rglob("*") は使用しない
- 拡張子指定（例: *.zip）による限定探索を行う
- MEDI_SHARED_SCAN_EXTS / MEDI_SHARED_EXTS により探索対象を制御する

【運用ポリシー】
- 自動実行を前提としない
- 必要なタイミングで手動実行する
- 冪等性を保ち、何度でも安全に再実行できることを前提とする

【前提環境変数】
- MEDI_SHARED_ROOT (必須)
- MEDI_SHARED_SCAN_EXTS / MEDI_SHARED_EXTS
- MEDI_SHARED_SCAN_LIMIT
- MEDI_SHARED_FACILITY_HINT_DEPTH
- MEDI_IMPORT_DB_*（DB接続）

【位置づけ】
共有 → 観測 → 判定 → コピー → 取込
のうち、「観測」フェーズを担う唯一の正本実装。
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
    """Return current timestamp string in JST with microseconds."""
    from zoneinfo import ZoneInfo

    jst = ZoneInfo("Asia/Tokyo")
    return datetime.now(tz=jst).strftime("%Y-%m-%d %H:%M:%S.%f")


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

    値は "zip,pdf,..." のカンマ区切り。
    default は安全側に倒して zip のみ（UNCの探索負荷を抑える）。
    """
    exts = (
        os.getenv("MEDI_SHARED_SCAN_EXTS")
        or os.getenv("MEDI_SHARED_EXTS")
        or "zip"
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
