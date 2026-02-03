# -*- coding: utf-8 -*-
"""
scripts/normalize_submit_kana.py

提出用CSV取込テーブルに対して、
氏名カナの揺れを正規化し name_kana_norm を UPDATE する。

前提:
- import_submit_csv.py で raw データは投入済み
- name_kana_norm カラムは既に存在する（無ければエラーで止める）
- INSERT は行わない（UPDATEのみ）
- 元のカナ列は更新しない（絶対に触らない）

.env（提出系に合わせる：勝手に変えない）
  SUBMIT_DB_HOST
  SUBMIT_DB_PORT
  SUBMIT_DB_NAME
  SUBMIT_DB_USER
  SUBMIT_DB_PASSWORD

  SUBMIT_TARGET_TABLE        : 取込先テーブル（推奨）

互換（存在すれば優先で読む）
  SUBMIT_IMPORT_TABLE        : 旧名（あればこれを使う）

任意（列名の上書きが必要なら指定）
  SUBMIT_IMPORT_ID_COL        (default: import_row_id)
  SUBMIT_IMPORT_NAMEKANA_COL  (default: 氏名（カナ）)
  SUBMIT_IMPORT_KANANORM_COL  (default: name_kana_norm)
  LOG_LEVEL                   (default: INFO)

任意（安全）
  SUBMIT_DRY_RUN              (default: false)
    - true の場合、UPDATEは実行せず「更新予定件数」まで出して終了
"""

from __future__ import annotations

import os
import sys
import logging
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

import unicodedata
import re
from typing import Optional, Any


# ------------------------------------------------------------
# bootstrap
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))


# ------------------------------------------------------------
# logging
# ------------------------------------------------------------
def setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger("normalize_submit_kana")
    logger.setLevel(getattr(logging, level, logging.INFO))

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ------------------------------------------------------------
# env utils
# ------------------------------------------------------------
def env_required(key: str) -> str:
    v = os.getenv(key)
    if not v or not v.strip():
        raise RuntimeError(f"必須環境変数 {key} が設定されていません")
    return v.strip()


def env_default(key: str, default: str) -> str:
    v = os.getenv(key)
    return v.strip() if v and v.strip() else default


def env_first(*keys: str) -> Optional[str]:
    """複数キーのうち最初に見つかった値を返す（互換用）"""
    for k in keys:
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    return None


def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# ------------------------------------------------------------
# db
# ------------------------------------------------------------
def connect_db():
    # 提出取込と同じ env 名に合わせる
    return mysql.connector.connect(
        host=env_required("SUBMIT_DB_HOST"),
        port=int(env_required("SUBMIT_DB_PORT")),
        database=env_required("SUBMIT_DB_NAME"),
        user=env_required("SUBMIT_DB_USER"),
        password=env_required("SUBMIT_DB_PASSWORD"),
        autocommit=False,
        use_pure=True,
    )


def dict_cursor(conn):
    return conn.cursor(dictionary=True, buffered=True)


def quote_ident(name: str) -> str:
    """
    カラム名/テーブル名の安全なバッククォート。
    - 文字列中の ` は `` にエスケープ
    """
    return f"`{name.replace('`', '``')}`"


def fetch_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
        """,
        (table,),
    )
    rows = cur.fetchall() or []
    cols: set[str] = set()
    for r in rows:
        if isinstance(r, dict):
            cols.add(str(r.get("COLUMN_NAME") or ""))
        else:
            try:
                cols.add(str(r[0]))
            except Exception:
                pass
    cols.discard("")
    return cols


# ------------------------------------------------------------
# kana normalization（割り切り版）
# ------------------------------------------------------------
_SMALL_KANA_MAP = str.maketrans(
    {
        "ァ": "ア",
        "ィ": "イ",
        "ゥ": "ウ",
        "ェ": "エ",
        "ォ": "オ",
        "ッ": "ツ",
        "ャ": "ヤ",
        "ュ": "ユ",
        "ョ": "ヨ",
        "ヮ": "ワ",
    }
)

# ダッシュ類をぜんぶ「ー」に寄せる
_DASH_PATTERN = re.compile(r"[‐-‒–—―−ーｰ]")

# ついでに「濁点/半濁点が分離してるやつ」をまとめる（念のため）
# NFKCでもだいたい吸えるが、現場CSVは念入りに。
_COMBINING_MARKS_RE = re.compile(r"[\u3099\u309A]")


def normalize_kana(src: Optional[str]) -> Optional[str]:
    if src is None:
        return None

    s = str(src)

    # 1) 全角化（NFKC）
    s = unicodedata.normalize("NFKC", s)

    # 2) 結合文字（゙゚）を除去（万一分離してたら）
    s = _COMBINING_MARKS_RE.sub("", s)

    # 3) 空白除去（全角・半角）
    s = s.replace(" ", "").replace("　", "")

    # 4) ダッシュ・長音の統一
    s = _DASH_PATTERN.sub("ー", s)

    # 5) 小さいカナを大きいカナへ
    s = s.translate(_SMALL_KANA_MAP)

    return s


def as_dict(row: Any) -> Optional[dict]:
    """
    mysql.connector の戻りが dict でも tuple でも来ても壊れないように。
    """
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return None


# ------------------------------------------------------------
# main
# ------------------------------------------------------------
def main() -> int:
    logger = setup_logger()

    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f".env loaded: {env_path}")

    # テーブル名は互換フォールバック（旧名があれば優先）
    table = env_first("SUBMIT_IMPORT_TABLE", "SUBMIT_TARGET_TABLE")
    if not table:
        raise RuntimeError("必須環境変数 SUBMIT_TARGET_TABLE（または SUBMIT_IMPORT_TABLE）が設定されていません")

    # デフォルトは「今のDDLに合わせて事故らない値」
    id_col = env_default("SUBMIT_IMPORT_ID_COL", "import_row_id")
    kana_col = env_default("SUBMIT_IMPORT_NAMEKANA_COL", "氏名（カナ）")
    norm_col = env_default("SUBMIT_IMPORT_KANANORM_COL", "name_kana_norm")

    dry_run = env_bool("SUBMIT_DRY_RUN", False)

    conn = connect_db()
    try:
        cur = dict_cursor(conn)

        logger.info(f"target table: {table}")
        logger.info(f"columns: id={id_col}, kana={kana_col}, norm={norm_col}")
        logger.info(f"dry_run={dry_run}")

        # --- 安全装置1: 列存在チェック（ここで落とす） ---
        cols = fetch_columns(cur, table)
        missing = [c for c in (id_col, kana_col, norm_col) if c not in cols]
        if missing:
            raise RuntimeError(f"テーブル列が見つかりません: {missing} (table={table})")

        q_table = quote_ident(table)
        q_id = quote_ident(id_col)
        q_kana = quote_ident(kana_col)
        q_norm = quote_ident(norm_col)

        # --- 事前サマリ（現状把握） ---
        cur.execute(
            f"""
            SELECT
              COUNT(*) AS total_rows,
              SUM(CASE WHEN {q_kana} IS NULL OR {q_kana}='' THEN 1 ELSE 0 END) AS kana_empty,
              SUM(CASE WHEN {q_norm} IS NULL OR {q_norm}='' THEN 1 ELSE 0 END) AS norm_empty,
              SUM(CASE WHEN {q_norm} = {q_kana} AND {q_kana} IS NOT NULL AND {q_kana}<>'' THEN 1 ELSE 0 END) AS norm_same,
              SUM(CASE WHEN {q_norm} IS NOT NULL AND {q_norm}<>'' AND {q_kana} IS NOT NULL AND {q_kana}<>'' AND {q_norm} <> {q_kana} THEN 1 ELSE 0 END) AS norm_diff
            FROM {q_table}
            """
        )
        pre = as_dict(cur.fetchone()) or {}
        logger.info(
            "pre-check: "
            f"total={pre.get('total_rows')} "
            f"kana_empty={pre.get('kana_empty')} "
            f"norm_empty={pre.get('norm_empty')} "
            f"norm_same={pre.get('norm_same')} "
            f"norm_diff={pre.get('norm_diff')}"
        )

        # --- 更新対象の取得（必要な行だけに絞る） ---
        # norm が空の行だけ更新（ラグ/負荷/無駄撃ち防止）
        cur.execute(
            f"""
            SELECT
              {q_id}   AS _id,
              {q_kana} AS _kana,
              {q_norm} AS _norm
            FROM {q_table}
            WHERE {q_kana} IS NOT NULL
              AND {q_kana} <> ''
              AND ({q_norm} IS NULL OR {q_norm} = '')
            """
        )
        rows = cur.fetchall() or []
        logger.info(f"rows fetched (need update): {len(rows)}")

        update_count = 0
        skip_bad_row = 0

        if dry_run:
            logger.info("DRY_RUN: no updates executed.")
            return 0

        # --- UPDATE 実行（元のカナ列は触らない） ---
        for raw in rows:
            r = as_dict(raw)
            if r is None:
                skip_bad_row += 1
                continue

            src = r.get("_kana")
            norm = normalize_kana(src)

            # 空なら空で埋めない（元が空じゃないはずだが保険）
            if norm is None or str(norm).strip() == "":
                continue

            cur.execute(
                f"""
                UPDATE {q_table}
                SET {q_norm} = %s
                WHERE {q_id} = %s
                """,
                (norm, r.get("_id")),
            )
            update_count += 1

            # 進捗ログ（重すぎない程度）
            if update_count % 5000 == 0:
                logger.info(f"updated: {update_count}")

        conn.commit()
        logger.info(f"updated rows: {update_count}")
        if skip_bad_row:
            logger.warning(f"skipped bad rows: {skip_bad_row}")

        # --- 安全装置2: コミット後の検算（その場で見える化） ---
        cur.execute(
            f"""
            SELECT
              COUNT(*) AS total_rows,
              SUM(CASE WHEN {q_kana} IS NULL OR {q_kana}='' THEN 1 ELSE 0 END) AS kana_empty,
              SUM(CASE WHEN {q_norm} IS NULL OR {q_norm}='' THEN 1 ELSE 0 END) AS norm_empty,
              SUM(CASE WHEN {q_norm} = {q_kana} AND {q_kana} IS NOT NULL AND {q_kana}<>'' THEN 1 ELSE 0 END) AS norm_same,
              SUM(CASE WHEN {q_norm} IS NOT NULL AND {q_norm}<>'' AND {q_kana} IS NOT NULL AND {q_kana}<>'' AND {q_norm} <> {q_kana} THEN 1 ELSE 0 END) AS norm_diff
            FROM {q_table}
            """
        )
        post = as_dict(cur.fetchone()) or {}
        logger.info(
            "post-check: "
            f"total={post.get('total_rows')} "
            f"kana_empty={post.get('kana_empty')} "
            f"norm_empty={post.get('norm_empty')} "
            f"norm_same={post.get('norm_same')} "
            f"norm_diff={post.get('norm_diff')}"
        )

        return 0

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
