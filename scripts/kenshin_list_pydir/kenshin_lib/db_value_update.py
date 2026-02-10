# -*- coding: utf-8 -*-
"""
kenshin_lib/db_value_update.py

【概要】
MySQL 上のテーブルから値を読み出し、変換（正規化など）して、同じテーブルの別カラムへ安全に UPDATE するための
汎用バッチ更新ユーティリティ。

【目的】
- テーブルや項目ごとの「値の置換・正規化」処理を、共通の枠組み（Job定義 + transform関数）で実行できるようにする。
- 変換結果が既存の保存先値と同じ場合は UPDATE しない（無駄な更新を避ける）。

【設計方針（安全第一）】
- 対象テーブル名は固定しない（Jobで指定）。
- UPDATE は主キー（または UNIQUE キー）列で絞り込む（誤って複数行更新しないため）。
  - `key_cols` に「行を一意に特定できる列」を必ず指定する。
- `where_sql` は WHERE 句の断片（先頭の WHERE は含めない）。
- `.env` を最小実装のローダーで読み込む（外部依存を増やさない）。

【環境変数（.env）】
- MYSQL_HOST（既定: 127.0.0.1）
- MYSQL_PORT（既定: 3306）
- MYSQL_USER（既定: root）
- MYSQL_PASSWORD（既定: 空）

【文字コード/照合順序】
- 接続は `utf8mb4` / `utf8mb4_ja_0900_as_cs` を前提に設定している。
  日本語カナや記号を含む照合を、プロジェクト方針に合わせるため。

【使い方】
- `UpdateJob` を作る（db名/テーブル/キー列/src_col/dst_col/where_sql など）。
- `transform(src_value)` を渡して `run_update_job()` を呼ぶ。
- まずは `dry_run=True` で件数と更新対象を確認してから本番実行する。

【固定化メモ】
- 本ファイルは「既に動作している処理」を前提に、仕様・前提を docstring に固定する目的で整備している。
- 挙動変更（SQLや更新条件の変更、アルゴリズム改修）は、別フェーズで行うこと。

"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Sequence, Any, Dict, List, Tuple, cast

import mysql.connector


# -----------------------------
# .env loader（依存なし）
# -----------------------------
def load_env(dotenv_path: str = ".env") -> None:
    """Load key=value pairs from a local .env file.

    - Lines starting with `#` are ignored.
    - Existing environment variables are not overwritten.
    - Quoted values (single/double) are unquoted.

    This loader is intentionally minimal to avoid adding external dependencies.
    """
    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def connect_mysql(db_name: str):
    """Create a MySQL connection.

    Connection parameters are read from environment variables:
      MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

    The connection uses utf8mb4 and `utf8mb4_ja_0900_as_cs` collation by default.
    """
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=db_name,
        autocommit=False,
        charset="utf8mb4",
        collation="utf8mb4_ja_0900_as_cs",
    )


# -----------------------------
# Update Job definition
# -----------------------------
@dataclass(frozen=True)
class UpdateJob:
    name: str
    db_name: str
    table: str
    key_cols: Sequence[str]   # PK or UNIQUE key columns used for safe updates
    src_col: str
    dst_col: str
    where_sql: str = ""       # SQL fragment without leading WHERE (optional)
    limit: int = 0
    chunk_size: int = 1000    # executemany batch size


def _select_sql(job: UpdateJob) -> str:
    cols = list(job.key_cols) + [job.src_col, job.dst_col]
    sql = f"SELECT {', '.join([f'`{c}`' for c in cols])} FROM `{job.table}`"
    if job.where_sql:
        sql += f" WHERE {job.where_sql}"
    if job.limit > 0:
        sql += f" LIMIT {job.limit}"
    return sql


def _update_sql(job: UpdateJob) -> str:
    where = " AND ".join([f"`{k}`=%s" for k in job.key_cols])
    return f"UPDATE `{job.table}` SET `{job.dst_col}`=%s WHERE {where}"


def run_update_job(
    job: UpdateJob,
    transform: Callable[[Any], Any],
    *,
    dotenv_path: str = ".env",
    dry_run: bool = False,
    verbose: bool = True,
) -> Dict[str, int]:
    """Run a batch update job.

    The job selects key columns plus (src_col, dst_col), applies `transform(src)` and
    updates dst_col only when the resulting value differs.

    Parameters
    - dotenv_path: path to `.env` (loaded only if present)
    - dry_run: if True, do not persist changes (transaction is rolled back)
    - verbose: if True, prints basic progress information

    Returns
      A dict with:
        selected: number of rows selected
        to_update: number of rows that would be updated
        updated: number of rows updated (sum of cursor.rowcount)
    """
    load_env(dotenv_path)
    conn = connect_mysql(job.db_name)

    selected = 0
    to_update = 0
    updated = 0

    cur = None
    try:
        cur = conn.cursor(dictionary=True)

        select_sql = _select_sql(job)
        if verbose:
            print(f"[{job.name}] SELECT: {select_sql}")

        cur.execute(select_sql)
        # mysql.connector の型推論が弱いので dict としてキャストする（Pylance対策）
        rows = cast(List[Dict[str, Any]], cur.fetchall())
        selected = len(rows)

        update_sql = _update_sql(job)
        batch: List[Tuple[Any, ...]] = []

        for r in rows:
            # src/dstはdictとして扱える
            raw = r.get(job.src_col)
            new_val = transform(raw)
            old_val = r.get(job.dst_col)

            # 変化がなければ更新しない
            if (old_val or "") == (new_val or ""):
                continue

            # キーが欠けてたら事故るので明示的に落とす
            key_values: List[Any] = []
            for k in job.key_cols:
                if k not in r:
                    raise KeyError(f"[{job.name}] key col not in row: {k}")
                key_values.append(r[k])

            params = [new_val] + key_values
            batch.append(tuple(params))

            if len(batch) >= job.chunk_size:
                to_update += len(batch)
                if not dry_run:
                    cur.executemany(update_sql, batch)
                    updated += cur.rowcount
                    conn.commit()
                batch.clear()

        if batch:
            to_update += len(batch)
            if not dry_run:
                cur.executemany(update_sql, batch)
                updated += cur.rowcount
                conn.commit()

        if dry_run:
            conn.rollback()

        if verbose:
            print(
                f"[{job.name}] selected={selected}, "
                f"to_update={to_update}, updated={updated}"
                f"{' (dry-run)' if dry_run else ''}"
            )

        return {"selected": selected, "to_update": to_update, "updated": updated}

    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        conn.close()
