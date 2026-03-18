

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_2_subscriber_identity_columns.py

Purpose:
- Recompute subscriber identity-related canonical columns using the shared
  normalization layer in common.py.
- This backfill exists because production data may still contain legacy values
  created before v1.0.2 normalization was fixed.

Target table:
- dev_phr.subscribers

Updated columns:
- insurance_symbol_match
- insurance_symbol_export
- insurance_number_match
- name_kana_full_match
- name_full_match

Notes:
- raw columns are never modified
- kanji normalization dictionary is applied through common.py
- the script updates only rows whose recalculated values differ
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable

# ------------------------------------------------------------
# VSCode Run ボタン (file実行) 対応
# ファイル直実行でも project root を import path に追加する
# ------------------------------------------------------------
import sys

_THIS_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _THIS_FILE.parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor
from scripts.work_folder.lib.normalize.common import (
    normalize_insurance_number_match,
    normalize_insurance_symbol_export,
    normalize_insurance_symbol_match,
    normalize_name_kana_match,
    normalize_name_kanji_match,
)


BATCH_SIZE = 1000

SELECT_SQL = """
SELECT
    id,
    insurance_symbol,
    insurance_number,
    name_kana_full,
    name_kanji_full,
    insurance_symbol_match,
    insurance_symbol_export,
    insurance_number_match,
    name_kana_full_match,
    name_full_match
FROM dev_phr.subscribers
ORDER BY id
"""

UPDATE_SQL = """
UPDATE dev_phr.subscribers
   SET insurance_symbol_match = %s,
       insurance_symbol_export = %s,
       insurance_number_match = %s,
       name_kana_full_match = %s,
       name_full_match = %s
 WHERE id = %s
"""


def _norm_db_value(value: Any) -> str | None:
    """DB値を比較用に正規化する。空文字は None と同じ扱いにする。"""
    if value is None:
        return None
    if isinstance(value, str):
        return value if value != "" else None
    return str(value)


def build_recomputed_values(cur, row: Dict[str, Any]) -> Dict[str, str | None]:
    """raw列から canonical 値を再計算する。"""
    raw_symbol = row.get("insurance_symbol") or ""
    raw_number = row.get("insurance_number") or ""
    raw_kana = row.get("name_kana_full") or ""
    raw_kanji = row.get("name_kanji_full") or ""

    return {
        "insurance_symbol_match": normalize_insurance_symbol_match(raw_symbol),
        "insurance_symbol_export": normalize_insurance_symbol_export(raw_symbol),
        "insurance_number_match": normalize_insurance_number_match(raw_number),
        "name_kana_full_match": normalize_name_kana_match(raw_kana),
        "name_full_match": normalize_name_kanji_match(raw_kanji, cur=cur),
    }


def needs_update(row: Dict[str, Any], recalculated: Dict[str, str | None]) -> bool:
    """現在値と再計算値を比較し、更新が必要か判定する。"""
    return any(
        _norm_db_value(row.get(col)) != _norm_db_value(recalculated.get(col))
        for col in recalculated.keys()
    )


def update_rows(*, dry_run: bool, limit: int | None) -> None:
    mysql_params = load_mysql_params()

    scanned = 0
    changed = 0
    update_params: list[tuple[Any, ...]] = []

    with connect_ctx(mysql_params, autocommit=False) as conn:
        cur = dict_cursor(conn)
        cur.execute(SELECT_SQL)

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                scanned += 1
                recalculated = build_recomputed_values(cur, row)

                if not needs_update(row, recalculated):
                    if limit is not None and scanned >= limit:
                        break
                    continue

                changed += 1
                update_params.append(
                    (
                        recalculated["insurance_symbol_match"],
                        recalculated["insurance_symbol_export"],
                        recalculated["insurance_number_match"],
                        recalculated["name_kana_full_match"],
                        recalculated["name_full_match"],
                        row["id"],
                    )
                )

                if len(update_params) >= BATCH_SIZE and not dry_run:
                    cur.executemany(UPDATE_SQL, update_params)
                    conn.commit()
                    update_params.clear()

                if limit is not None and scanned >= limit:
                    break

            if limit is not None and scanned >= limit:
                break

        if update_params and not dry_run:
            cur.executemany(UPDATE_SQL, update_params)
            conn.commit()

    print(f"scanned={scanned}")
    print(f"changed={changed}")
    print(f"dry_run={dry_run}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill subscriber identity canonical columns for v1.0.2"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="更新はせず、変更件数のみ確認する",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="先頭から指定件数だけ確認・更新する",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_rows(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()