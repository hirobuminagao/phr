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

This script is intended to be executed **once during migration**.
"""

import argparse
from pathlib import Path
import sys

# ------------------------------------------------------------
# project root import path
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.work_folder.lib.db.mysql import connect_db
from scripts.work_folder.lib.normalize.common import (
    normalize_kana_full_match,
    normalize_name_full_match,
    normalize_symbol_match,
    normalize_number_match,
)


# ------------------------------------------------------------
# processing
# ------------------------------------------------------------

def main() -> int:

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", type=int, default=0)
    args = ap.parse_args()

    con = connect_db()
    cur = con.cursor(dictionary=True)

    cur.execute(
        """
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
    )

    rows = cur.fetchall()

    total = len(rows)
    print(f"[INFO] rows needing backfill = {total}")

    updated = 0

    for i, row in enumerate(rows, start=1):

        kana_match = normalize_kana_full_match(row["name_kana_full"])
        name_match = normalize_name_full_match(row["name_kanji_full"])
        symbol_match = normalize_symbol_match(row["insurance_symbol"])
        number_match = normalize_number_match(row["insurance_number"])

        cur.execute(
            """
            UPDATE subscribers
            SET
                name_kana_full_match = %s,
                name_full_match = %s,
                insurance_symbol_match = %s,
                insurance_number_match = %s
            WHERE id = %s
            """,
            (
                kana_match,
                name_match,
                symbol_match,
                number_match,
                row["id"],
            ),
        )

        updated += 1

        if args.limit and updated >= args.limit:
            break

        if i % 1000 == 0:
            print(f"[PROGRESS] processed {i}/{total}")

    if args.dry_run:
        con.rollback()
        print(f"[DRY-RUN] updates={updated}")
    else:
        con.commit()
        print(f"[OK] updates={updated}")

    con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
