# -*- coding: utf-8 -*-
"""
scripts/import_submit_csv.py

【目的】
企業個別の提出用CSV（例: symbol100）を、そのまま MySQL(work_other) の取込テーブルへ入れる。
- 取込テーブルは残し続ける（最新CSVが来たら TRUNCATE → 再取込）
- CSVは inbox に 1ファイルだけ置く（複数あったらエラー）
- CSVヘッダーは csv_header_map_submit.display_order の順と一致する前提（一致しなければエラー）
  ※ map 側の csv_header / original_header は「改行/余分な空白が混じる」想定なので、
     CSV側も同様に正規化して比較する

【重要】数値列の扱い
- INT/DECIMAL/FLOAT系の列に "測定不可" / "未実施" / "H" / "L" などが入ることがある
- 仕様確認済み: これらは NULL でよい
→ information_schema から「数値カラム」を自動判定し、数値以外は None に落として INSERT する

env（必須）※ここは .env に既にある名前に固定（勝手に変えない）
  SUBMIT_INBOX_ROOT         : CSV置き場フォルダ（BASE_DIRからの相対 or 絶対）
                              例) submit_inbox/06139463/symbol100
  SUBMIT_TARGET_TABLE       : 取込先テーブル名 例) symbol100_all_20260127

  SUBMIT_DB_HOST
  SUBMIT_DB_PORT
  SUBMIT_DB_NAME
  SUBMIT_DB_USER
  SUBMIT_DB_PASSWORD

env（任意）
  SUBMIT_TRUNCATE           : true/false (default true)
  SUBMIT_CSV_FILENAME       : 取込対象CSVのファイル名（指定時はそれを使う）
                              未指定時は inbox 内の *.csv が1つだけであることを要求
  SUBMIT_INSERT_BATCH       : executemany のバッチサイズ (default 1000)
  LOG_LEVEL                 : INFO/DEBUG... (default INFO)
"""

from __future__ import annotations

# --- path bootstrap (MUST be before importing local libs, if any) ---
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # = kenshin_list_pydir
sys.path.insert(0, str(BASE_DIR))
# ------------------------------------------------------------

import os
import csv
import re
import logging
from typing import Any, Optional, Sequence, Iterator, List, Dict, cast

from dotenv import load_dotenv
import mysql.connector


# ============================================================
# logging / env utils
# ============================================================
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

    logger = logging.getLogger("import_submit_csv")
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


# ============================================================
# DB connect
# ============================================================
def load_submit_db_params() -> dict[str, Any]:
    host = env_required("SUBMIT_DB_HOST")
    port = int(env_required("SUBMIT_DB_PORT"))
    name = env_required("SUBMIT_DB_NAME")
    user = env_required("SUBMIT_DB_USER")
    password = env_required("SUBMIT_DB_PASSWORD")

    return {
        "host": host,
        "port": port,
        "database": name,
        "user": user,
        "password": password,
        "autocommit": False,
        "use_pure": True,
    }


def connect_submit(params: dict[str, Any]):
    # mysql.connector.connect の戻り型は typeshed 上ブレるので、型注釈は付けない（Pylance回避）
    return mysql.connector.connect(**params)


def dict_cursor(conn):
    # dictionary=True を強制（ただし typeshed が不安定なので Any 扱い）
    return conn.cursor(dictionary=True, buffered=True)


# ============================================================
# CSV helpers
# ============================================================
def _resolve_path(p: str) -> Path:
    raw = (p or "").strip()
    if raw == "":
        raise RuntimeError("空のパスが指定されました")
    path = Path(raw)
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()


def find_single_csv(inbox_dir: Path, filename: Optional[str]) -> Path:
    if not inbox_dir.exists() or not inbox_dir.is_dir():
        raise RuntimeError(f"INBOX が存在しないかディレクトリではありません: {inbox_dir}")

    if filename:
        p = inbox_dir / filename
        if not p.exists() or not p.is_file():
            raise RuntimeError(f"SUBMIT_CSV_FILENAME が見つかりません: {p}")
        return p

    cands = sorted([p for p in inbox_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])
    if len(cands) == 0:
        raise RuntimeError(f"inbox 内にCSVがありません: {inbox_dir}")
    if len(cands) >= 2:
        names = ", ".join([p.name for p in cands[:10]])
        raise RuntimeError(f"inbox 内のCSVが複数あります（1つにしてください）: count={len(cands)} sample={names}")
    return cands[0]


def _try_open_csv(csv_path: Path, encoding: str):
    f = csv_path.open("r", encoding=encoding, newline="")
    r = csv.reader(f)
    _ = next(r, None)  # 1行読んで戻す（encoding確認）
    f.seek(0)
    r = csv.reader(f)
    return f, r


def open_csv_reader(csv_path: Path) -> tuple[Any, Iterator[List[str]]]:
    """
    日本語CSVの想定:
    - UTF-8-SIG / UTF-8 / CP932(Shift-JIS) が混在しがち
    まず utf-8-sig → だめなら cp932 にフォールバック
    """
    try:
        f, r = _try_open_csv(csv_path, "utf-8-sig")
        return f, cast(Iterator[List[str]], r)
    except Exception:
        try:
            f, r = _try_open_csv(csv_path, "cp932")
            return f, cast(Iterator[List[str]], r)
        except Exception as e:
            raise RuntimeError(f"CSVを開けません（encoding不明）: {csv_path} err={e}")


# ============================================================
# header normalization
# ============================================================
# ★ヘッダー照合用途: 空白差（改行/タブ/全角空白/半角空白）は無意味なので「全部除去」する
_WS_ALL_RE = re.compile(r"\s+")

def norm_header(s: str) -> str:
    t = (s or "").replace("\ufeff", "")
    # 全角スペースもまとめて空白扱いにしたいので、先に半角へ寄せる
    t = t.replace("　", " ")
    t = t.strip()
    # 連続空白を1個…ではなく、今回の用途は「全除去」
    t = _WS_ALL_RE.sub("", t)
    return t


def diff_headers(expected: List[str], got: List[str], limit: int = 50) -> List[str]:
    diffs: List[str] = []
    for i, (e, g) in enumerate(zip(expected, got), start=1):
        if e != g:
            diffs.append(f"[{i}] expected={e!r} got={g!r}")
            if len(diffs) >= limit:
                diffs.append("...（差分が多いので省略）")
                break
    return diffs


# ============================================================
# numeric coercion
# ============================================================
# maketrans の長さ事故を起こさないよう、辞書で定義（安全）
_Z2H_TABLE = {
    ord("０"): "0",
    ord("１"): "1",
    ord("２"): "2",
    ord("３"): "3",
    ord("４"): "4",
    ord("５"): "5",
    ord("６"): "6",
    ord("７"): "7",
    ord("８"): "8",
    ord("９"): "9",
    ord("－"): "-",  # 全角マイナス
    ord("ー"): "-",  # 長音をマイナス扱い（数字列の - として）
    ord("＋"): "+",  # 全角プラス
    ord("．"): ".",  # 全角ドット
    ord("，"): ",",  # 全角カンマ
    ord("　"): " ",  # 全角スペース
}

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _norm_num_text(v: object) -> str:
    s = str(v).strip()
    return s.translate(_Z2H_TABLE).strip()


def to_int_or_none(v: object) -> int | None:
    if v is None:
        return None
    s = _norm_num_text(v)
    if s == "":
        return None
    if not _INT_RE.fullmatch(s):
        return None
    try:
        return int(s)
    except Exception:
        return None


def to_float_or_none(v: object) -> float | None:
    if v is None:
        return None
    s = _norm_num_text(v)
    if s == "":
        return None
    if "," in s:
        return None
    if not _FLOAT_RE.fullmatch(s):
        return None
    try:
        return float(s)
    except Exception:
        return None


def empty_to_none(v: object) -> object:
    if v is None:
        return None
    s = str(v)
    if s.strip() == "":
        return None
    return v


# ============================================================
# mapping / schema helpers
# ============================================================
def fetch_header_map(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT display_order, csv_header, table_column, original_header
        FROM csv_header_map_submit
        ORDER BY display_order ASC
        """
    )
    rows_any = cur.fetchall()
    rows = cast(List[Dict[str, Any]], rows_any)
    if not rows:
        raise RuntimeError("csv_header_map_submit にデータがありません")
    return [dict(r) for r in rows]


def fetch_numeric_columns(cur, table: str) -> tuple[set[str], set[str]]:
    """
    information_schema からテーブルの数値カラムを取得
    - int系: int/bigint/smallint/mediumint/tinyint
    - float系: float/double/decimal
    """
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
        """,
        (table,),
    )
    rows_any = cur.fetchall()
    rows = cast(List[Dict[str, Any]], rows_any)

    int_cols: set[str] = set()
    float_cols: set[str] = set()

    for r in rows:
        col = str(r.get("COLUMN_NAME") or "")
        dt = str(r.get("DATA_TYPE") or "").lower()
        if col == "":
            continue
        if dt in ("int", "bigint", "smallint", "mediumint", "tinyint"):
            int_cols.add(col)
        elif dt in ("float", "double", "decimal"):
            float_cols.add(col)

    return int_cols, float_cols


def build_insert_sql(table: str, table_columns: Sequence[str]) -> str:
    cols = ", ".join([f"`{c}`" for c in table_columns])
    ph = ", ".join(["%s"] * len(table_columns))
    return f"INSERT INTO `{table}` ({cols}) VALUES ({ph})"


# ============================================================
# main
# ============================================================
def main() -> int:
    logger = setup_logger()

    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f".env loaded: {env_path}")
    else:
        logger.warning(f".env not found: {env_path}（環境変数を直接使用）")

    # ★ここをenvに合わせて固定（勝手に変えない）
    inbox_dir = env_required("SUBMIT_INBOX_ROOT")
    table = env_required("SUBMIT_TARGET_TABLE")

    truncate = env_bool("SUBMIT_TRUNCATE", True)
    csv_filename = os.getenv("SUBMIT_CSV_FILENAME", "").strip() or None
    batch_size = env_int("SUBMIT_INSERT_BATCH", 1000)

    inbox_path = _resolve_path(inbox_dir)
    csv_path = find_single_csv(inbox_path, csv_filename)

    params = load_submit_db_params()

    logger.info(f"BASE_DIR={BASE_DIR}")
    logger.info(f"INBOX={inbox_path}")
    logger.info(f"TABLE={table}")
    logger.info(f"TRUNCATE={truncate}")
    logger.info(f"CSV={csv_path}")
    logger.info(f"DB={params['host']}:{params['port']}/{params['database']} user={params['user']}")

    conn = connect_submit(params)
    try:
        cur = dict_cursor(conn)

        # 1) map取得（CSVの期待ヘッダー順 / INSERT対象カラム順）
        mapping = fetch_header_map(cur)

        # primary: csv_header
        expected_headers = [norm_header(str(r.get("csv_header") or "")) for r in mapping]

        # alt: original_header があればそれ、無ければ csv_header
        expected_headers_alt = [
            norm_header(str((r.get("original_header") or r.get("csv_header") or ""))) for r in mapping
        ]

        table_columns = [str(r.get("table_column") or "") for r in mapping]
        if any(c.strip() == "" for c in table_columns):
            raise RuntimeError("csv_header_map_submit.table_column に空が混じっています（mapを確認）")

        # 2) CSVヘッダー読み込み & 検証（csv_header / original_header のどちらでもOK）
        f, reader = open_csv_reader(csv_path)
        try:
            header_row = next(reader, None)
            if header_row is None:
                raise RuntimeError("CSVが空です（ヘッダー行がありません）")

            got_headers = [norm_header(h) for h in header_row]

            if len(got_headers) != len(expected_headers):
                raise RuntimeError(
                    f"CSVヘッダー列数が map と一致しません: got={len(got_headers)} expected={len(expected_headers)}"
                )

            if got_headers == expected_headers:
                logger.info("CSVヘッダー検証 OK（map.csv_header と一致）")
            elif got_headers == expected_headers_alt:
                logger.info("CSVヘッダー検証 OK（map.original_header と一致）")
            else:
                diffs_primary = diff_headers(expected_headers, got_headers, limit=50)
                diffs_alt = diff_headers(expected_headers_alt, got_headers, limit=10)
                msg = (
                    "CSVヘッダーが map と一致しません（順序/表記が違います）\n"
                    "[primary: csv_header との差分]\n" + "\n".join(diffs_primary)
                    + "\n\n[alt: original_header との差分(先頭10件)]\n" + "\n".join(diffs_alt)
                )
                raise RuntimeError(msg)

        finally:
            try:
                f.close()
            except Exception:
                pass

        # 3) 数値カラムをDBから自動判定
        int_cols, float_cols = fetch_numeric_columns(cur, table)

        # 4) TRUNCATE
        if truncate:
            logger.info("TRUNCATE start")
            cur.execute(f"TRUNCATE TABLE `{table}`")
            conn.commit()
            logger.info("TRUNCATE done")

        # 5) INSERT 準備
        insert_sql = build_insert_sql(table, table_columns)
        logger.info("INSERT SQL prepared")

        # 6) CSV読み込み直して投入
        f2, reader2 = open_csv_reader(csv_path)
        try:
            _ = next(reader2, None)  # ヘッダー捨て

            batch: list[tuple[Any, ...]] = []
            inserted = 0
            row_no = 0

            for row in reader2:
                row_no += 1

                if len(row) != len(table_columns):
                    raise RuntimeError(
                        f"CSV行の列数がヘッダーと一致しません: row={row_no} got={len(row)} expected={len(table_columns)}"
                    )

                values: list[Any] = list(row)
                values = [empty_to_none(v) for v in values]

                for i, col in enumerate(table_columns):
                    v = values[i]
                    if v is None:
                        continue
                    if col in int_cols:
                        values[i] = to_int_or_none(v)
                    elif col in float_cols:
                        values[i] = to_float_or_none(v)

                batch.append(tuple(values))

                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    inserted += len(batch)
                    logger.info(f"INSERT committed: +{len(batch)} (total={inserted})")
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                logger.info(f"INSERT committed: +{len(batch)} (total={inserted})")
                batch.clear()

            logger.info(f"DONE: inserted={inserted}")
            return 0

        finally:
            try:
                f2.close()
            except Exception:
                pass

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
