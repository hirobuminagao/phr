#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_1_subscriber_match_columns.py

Purpose
-------
Backfill match columns added in PHR v1.0.1 for existing rows in dev_phr.subscribers.

This script calculates the following columns for rows where they are NULL:

- name_kana_full_match
- name_full_match
- insurance_symbol_match
- insurance_number_match

These values are derived from the original columns using the same normalization
rules used by the application layer.

This script is intended to be executed once during migration.
"""

from __future__ import annotations

from typing import Any, cast

import argparse
import sys
from pathlib import Path

# ------------------------------------------------------------
# project root import path
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor
from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_match,
)


# ------------------------------------------------------------
# local match normalization
# ------------------------------------------------------------

def normalize_name_full_match(value: str) -> str:
    """漢字氏名 match 用: 半角/全角スペース除去。"""
    return (value or "").replace(" ", "").replace("　", "").strip()



def normalize_name_kana_full_match(value: str) -> str:
    """カナ氏名 match 用: 半角/全角スペース + 中点除去。"""
    return (
        (value or "")
        .replace(" ", "")
        .replace("　", "")
        .replace("・", "")
        .replace("･", "")
        .strip()
    )



# ------------------------------------------------------------
# processing
# ------------------------------------------------------------

def as_text(value: Any) -> str:
    """DB 値を match 正規化入力用の文字列へ寄せる。None は空文字。"""
    if value is None:
        return ""
    return str(value)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default=None, help="接続先 DB スキーマ名")
    ap.add_argument("--limit", type=int, default=0, help="更新する件数上限 (0 = 無制限)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    params = load_mysql_params()
    if args.schema:
        params.database = args.schema

    with connect_ctx(params) as con:
        cur = dict_cursor(con)

        sql = """
            SELECT
                id,
                name_kana_full,
                name_kanji_full,
                insurance_symbol,
                insurance_number
            FROM subscribers
            WHERE
                name_kana_full_match IS NULL
                OR name_full_match IS NULL
                OR insurance_symbol_match IS NULL
                OR insurance_number_match IS NULL
            ORDER BY id
        """
        if args.limit > 0:
            sql += " LIMIT %s"
            cur.execute(sql, (args.limit,))
        else:
            cur.execute(sql)

        rows = cast(list[dict[str, Any]], list(cur.fetchall()))
        total = len(rows)
        print(f"[INFO] rows needing backfill = {total}")
        print(f"[INFO] DB_SCHEMA = {params.database}")
        print(f"[INFO] DRY_RUN   = {args.dry_run}")
        print(f"[INFO] LIMIT     = {args.limit}")

        updated = 0

        for i, row in enumerate(rows, start=1):
            kana_match = normalize_name_kana_full_match(as_text(row["name_kana_full"]))
            name_match = normalize_name_full_match(as_text(row["name_kanji_full"]))
            symbol_match = normalize_insurance_symbol_match(as_text(row["insurance_symbol"]))
            number_match = normalize_insurance_number_match(as_text(row["insurance_number"]))
            row_id = int(row["id"])

            cur.execute(
                """
                UPDATE subscribers
                SET
                    name_kana_full_match = %s,
                    name_full_match = %s,
                    insurance_symbol_match = %s,
                    insurance_number_match = %s,
                    updated_at = NOW(3)
                WHERE id = %s
                """,
                (
                    kana_match,
                    name_match,
                    symbol_match,
                    number_match,
                    row_id,
                ),
            )

            updated += 1

            if i % 1000 == 0:
                print(f"[PROGRESS] processed {i}/{total}")

        if args.dry_run:
            con.rollback()
            print(f"[DRY-RUN] updates={updated}")
        else:
            con.commit()
            print(f"[OK] updates={updated}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
