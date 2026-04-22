#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import argparse
from dataclasses import dataclass
from typing import Any

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

"""
v1_1_0_reset_subscriber_name_parts_to_null_if_invalid.py

目的:
- subscribers の name parts 列に残っている不正な暫定値を NULL に補正する

背景:
- 旧仕様では、split 不可の氏名に対して full 相当値が
  name_kana_given / name_kanji_given などへ入るケースがあった
- v1.1.0 では「split できたときだけ parts を保持し、split 不可なら full のみ保持」へ方針変更した
- そのため、既存 subscribers の暫定 parts を一度 NULL に戻し、以後は正しい split ソースから再補完可能な状態へ揃える

判定方針:
- 未設定は NULL または空文字として扱う
- full が family または given にだけ入っており、他 parts が未設定なら「invalid placeholder」とみなす
- 該当時は family / middle / given をすべて NULL にする

注意:
- 本スクリプトは subscribers 本体を直接更新する backfill である
- 初回は --dry-run で対象件数を確認してから本実行すること
"""


@dataclass
class UpdateResult:
    label: str
    matched_count: int
    updated_count: int



def is_blank_sql(column: str) -> str:
    return f"({column} IS NULL OR TRIM({column}) = '')"



def build_invalid_parts_where_clause(full_col: str, family_col: str, middle_col: str, given_col: str) -> str:
    family_blank = is_blank_sql(family_col)
    middle_blank = is_blank_sql(middle_col)
    given_blank = is_blank_sql(given_col)
    full_present = f"{full_col} IS NOT NULL AND TRIM({full_col}) <> ''"

    family_eq_full = f"TRIM({family_col}) = TRIM({full_col})"
    given_eq_full = f"TRIM({given_col}) = TRIM({full_col})"

    family_placeholder = f"({full_present} AND {family_eq_full} AND {middle_blank} AND {given_blank})"
    given_placeholder = f"({full_present} AND {family_blank} AND {middle_blank} AND {given_eq_full})"

    return f"({family_placeholder} OR {given_placeholder})"



def count_targets(cur: Any, where_clause: str) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {DEV_PHR}.subscribers
        WHERE {where_clause}
        """
    )
    row = cur.fetchone()
    if not row:
        return 0
    if isinstance(row, dict):
        return int(row.get("cnt", 0))
    return int(row[0])



def update_targets(
    cur: Any,
    *,
    where_clause: str,
    family_col: str,
    middle_col: str,
    given_col: str,
) -> int:
    cur.execute(
        f"""
        UPDATE {DEV_PHR}.subscribers
        SET
            {family_col} = NULL,
            {middle_col} = NULL,
            {given_col} = NULL,
            updated_at = NOW(3)
        WHERE {where_clause}
        """
    )
    return int(cur.rowcount)



def run_one(
    cur: Any,
    *,
    label: str,
    full_col: str,
    family_col: str,
    middle_col: str,
    given_col: str,
    dry_run: bool,
) -> UpdateResult:
    where_clause = build_invalid_parts_where_clause(full_col, family_col, middle_col, given_col)
    matched_count = count_targets(cur, where_clause)

    updated_count = 0
    if not dry_run and matched_count > 0:
        updated_count = update_targets(
            cur,
            where_clause=where_clause,
            family_col=family_col,
            middle_col=middle_col,
            given_col=given_col,
        )

    return UpdateResult(
        label=label,
        matched_count=matched_count,
        updated_count=updated_count,
    )



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="更新せず対象件数のみ確認する")
    args = parser.parse_args()

    base_params = load_mysql_base_params()

    with connect_ctx(base_params, database=DEV_PHR, autocommit=False) as conn:
        cur = dict_cursor(conn)
        try:
            results = [
                run_one(
                    cur,
                    label="kana_parts",
                    full_col="name_kana_full",
                    family_col="name_kana_family",
                    middle_col="name_kana_middle",
                    given_col="name_kana_given",
                    dry_run=args.dry_run,
                ),
                run_one(
                    cur,
                    label="kanji_parts",
                    full_col="name_kanji_full",
                    family_col="name_kanji_family",
                    middle_col="name_kanji_middle",
                    given_col="name_kanji_given",
                    dry_run=args.dry_run,
                ),
            ]

            total_matched = sum(r.matched_count for r in results)
            total_updated = sum(r.updated_count for r in results)

            print("=== backfill result ===")
            print(f"dry_run: {args.dry_run}")
            for r in results:
                print(f"label={r.label} matched={r.matched_count} updated={r.updated_count}")
            print(f"total_matched={total_matched}")
            print(f"total_updated={total_updated}")

            if args.dry_run:
                conn.rollback()
                print("dry-run rollback done")
            else:
                conn.commit()
                print("commit done")
        except Exception:
            conn.rollback()
            print("rollback done (error)")
            raise
        finally:
            cur.close()


if __name__ == "__main__":
    main()