# -*- coding: utf-8 -*-
"""
kenshin_lib/db_value_update.py

mysql-connector-python を使って、
DBから値を読み込み → 変換 → 同一行に UPDATE する汎用ユーティリティ。

- テーブル固定しない
- PK（または一意キー）で安全に更新
- .env から接続情報を取得（python-dotenv不要）
- Pylance の型警告を潰す（dictキャスト）
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
    """
    シンプルdotenvローダ。
    既に環境変数にあるキーは上書きしない。
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
    """
    接続は .env の以下を参照：
      MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
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
    key_cols: Sequence[str]   # PK or UNIQUE
    src_col: str
    dst_col: str
    where_sql: str = ""
    limit: int = 0
    chunk_size: int = 1000


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
    """
    実行結果:
      selected: SELECT 件数
      to_update: UPDATE 対象件数
      updated: 実更新件数（cursor.rowcount の合計）
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
