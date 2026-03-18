#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_2_hia_dashboard_identity_columns.py

Purpose:
- Backfill dashboard-side subscriber enrichment columns and `name_match`
  using the SAME logic as hia_import_dashboard_csv.py.
- `snapshot_identity_key` is NOT updated here because legacy rows can
  collapse to the same normalized key and violate the unique constraint.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Mapping, cast

import sys

_THIS_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _THIS_FILE.parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor

# ★ここ重要：本体ロジックをそのまま使う
from scripts.work_folder.scripts.hia_import_dashboard_csv import (
    normalize_dashboard_row,
    fetch_subscriber_enrichment,
    build_row_sha,
)

BATCH_SIZE = 1000

SELECT_SQL = """
SELECT *
FROM work_other.hia_dashboard_status
WHERE hia_dashboard_person_id > %s
ORDER BY hia_dashboard_person_id
LIMIT %s
"""

UPDATE_SQL = """
UPDATE work_other.hia_dashboard_status
   SET name_match = %s,
       subscriber_person_id_custom = %s,
       subscriber_name_kana_full = %s,
       subscriber_name_kana_full_match = %s,
       subscriber_gender_code = %s,
       subscriber_birth = %s,
       identity_hash = %s,
       row_sha256 = %s,
       updated_at = CURRENT_TIMESTAMP(3)
 WHERE hia_dashboard_person_id = %s
"""

RowDict = Mapping[str, Any]


def build_from_existing_row(lookup_cur, row: RowDict) -> dict[str, Any]:
    """
    既存row → 正規ロジックに乗せ直す
    """

    # ① raw的に復元（最低限）
    raw = {
        "insurer_number": row.get("insurer_number"),
        "insurance_symbol": row.get("insurance_symbol"),
        "insurance_number": row.get("insurance_number"),
        "relationship": row.get("relationship"),
        "branch_number": row.get("branch_number"),
        "name": row.get("name"),
        "status": row.get("status"),
        "reservation_date": row.get("reservation_date"),
        "exam_date": row.get("exam_date"),
        "company_name": row.get("company_name"),
        "department_name": row.get("department_name"),
        "medical_institution": row.get("medical_institution"),
        "course_name": row.get("course_name"),
        "employee_number": row.get("employee_number"),
        "email": row.get("email"),
        "reminder_send_count": row.get("reminder_send_count"),
        "exclusion_reason": row.get("exclusion_reason"),
    }

    # ② normalize
    normalized = normalize_dashboard_row(
        raw,
        insurer_number=str(raw.get("insurer_number") or ""),
        cur=lookup_cur,
    )

    # ③ subscriber enrichment
    enrichment = fetch_subscriber_enrichment(
        lookup_cur,
        insurer_number=normalized["insurer_number"],
        insurance_symbol_match=normalized["insurance_symbol_match"],
        insurance_number_match=normalized["insurance_number_match"],
        name_full_match=normalized["name_match"],
    )

    normalized.update(enrichment)

    # ⑤ row sha
    normalized["row_sha256"] = build_row_sha(normalized)

    return normalized


def needs_update(row: RowDict, recalculated: Mapping[str, Any]) -> bool:
    cols = [
        "name_match",
        "subscriber_person_id_custom",
        "subscriber_name_kana_full",
        "subscriber_name_kana_full_match",
        "subscriber_gender_code",
        "subscriber_birth",
        "identity_hash",
        "row_sha256",
    ]

    for c in cols:
        if (row.get(c) or None) != (recalculated.get(c) or None):
            return True
    return False


def update_rows(*, dry_run: bool, limit: int | None) -> None:
    mysql_params = load_mysql_params()

    scanned = 0
    changed = 0
    last_id = 0

    with connect_ctx(mysql_params, autocommit=True) as read_conn, \
         connect_ctx(mysql_params, autocommit=True) as lookup_conn, \
         connect_ctx(mysql_params, autocommit=False) as write_conn:

        read_cur = dict_cursor(read_conn)
        lookup_cur = dict_cursor(lookup_conn)
        write_cur = dict_cursor(write_conn)

        while True:
            batch_limit = BATCH_SIZE
            if limit is not None:
                remaining = limit - scanned
                if remaining <= 0:
                    break
                batch_limit = min(BATCH_SIZE, remaining)

            read_cur.execute(SELECT_SQL, (last_id, batch_limit))
            rows = read_cur.fetchall() or []
            if not rows:
                break

            params = []

            for row_any in rows:
                row = row_any if isinstance(row_any, Mapping) else None
                if row is None:
                    raise TypeError(f"Unexpected row type from cursor: {type(row_any)!r}")

                row_id_any = row.get("hia_dashboard_person_id")
                if row_id_any is None:
                    raise ValueError("hia_dashboard_person_id is required for backfill")

                row_id = cast(int, row_id_any)
                last_id = row_id
                scanned += 1

                recalculated = build_from_existing_row(lookup_cur, row)
                print(
                    "DEBUG RAW:",
                    repr(row.get("name")),
                    "| MATCH =>",
                    repr(recalculated.get("name_match")),
                    "| symbol:",
                    repr(row.get("insurance_symbol")),
                    "| number:",
                    repr(row.get("insurance_number")),
                )

                if not needs_update(row, recalculated):
                    continue

                changed += 1

                params.append(
                    (
                        recalculated["name_match"],
                        recalculated["subscriber_person_id_custom"],
                        recalculated["subscriber_name_kana_full"],
                        recalculated["subscriber_name_kana_full_match"],
                        recalculated["subscriber_gender_code"],
                        recalculated["subscriber_birth"],
                        recalculated["identity_hash"],
                        recalculated["row_sha256"],
                        row_id,
                    )
                )

            if params and not dry_run:
                write_cur.executemany(UPDATE_SQL, params)
                write_conn.commit()

    print(f"scanned={scanned}")
    print(f"changed={changed}")
    print(f"dry_run={dry_run}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    update_rows(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()