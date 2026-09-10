#!/usr/bin/env python3
"""Delete legacy-key dashboard rows only when the corresponding HIA-ID key row exists."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--insurer-number", default="", help="Optional insurer number scope.")
    parser.add_argument("--work-db", default="work_other")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--apply", action="store_true", help="Delete rows. Omit for dry-run.")
    return parser.parse_args()


def find_duplicate_legacy_rows(cur: Any, *, work_db: str, insurer_number: str = "") -> list[dict[str, Any]]:
    insurer_clause = "AND legacy.insurer_number = %s" if insurer_number else ""
    params = (insurer_number,) if insurer_number else ()
    cur.execute(
        f"""
        SELECT legacy.hia_dashboard_person_id AS legacy_id,
               current_row.hia_dashboard_person_id AS current_id,
               legacy.insurer_number, legacy.subscribers_id, legacy.hia_subscriber_id,
               legacy.snapshot_identity_key AS legacy_key,
               current_row.snapshot_identity_key AS current_key,
               legacy.is_active AS legacy_is_active,
               legacy.updated_at AS legacy_updated_at,
               current_row.updated_at AS current_updated_at
        FROM `{work_db}`.hia_dashboard_status legacy
        INNER JOIN `{work_db}`.hia_dashboard_status current_row
          ON current_row.insurer_number = legacy.insurer_number
         AND current_row.hia_subscriber_id = legacy.hia_subscriber_id
         AND current_row.hia_dashboard_person_id <> legacy.hia_dashboard_person_id
         AND current_row.snapshot_identity_key = SHA2(
               CONCAT(current_row.insurer_number, '|HIA_SUBSCRIBER_ID|', current_row.hia_subscriber_id), 256
             )
        WHERE legacy.hia_subscriber_id IS NOT NULL
          AND legacy.hia_subscriber_id <> ''
          AND legacy.snapshot_identity_key <> SHA2(
                CONCAT(legacy.insurer_number, '|HIA_SUBSCRIBER_ID|', legacy.hia_subscriber_id), 256
              )
          {insurer_clause}
        ORDER BY legacy.insurer_number, legacy.hia_dashboard_person_id
        """,
        params,
    )
    return [dict(row) for row in cur.fetchall()]


def delete_rows(cur: Any, *, work_db: str, row_ids: list[int]) -> int:
    if not row_ids:
        return 0
    placeholders = ", ".join(["%s"] * len(row_ids))
    cur.execute(
        f"DELETE FROM `{work_db}`.hia_dashboard_status WHERE hia_dashboard_person_id IN ({placeholders})",
        tuple(row_ids),
    )
    return int(cur.rowcount)


def main() -> int:
    args = parse_args()
    if not args.work_db.replace("_", "").isalnum():
        raise ValueError("invalid work-db")
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.work_db, autocommit=False) as conn:
        cur = dict_cursor(conn)
        rows = find_duplicate_legacy_rows(
            cur, work_db=args.work_db, insurer_number=str(args.insurer_number or "").strip()
        )
        deleted = 0
        if args.apply:
            deleted = delete_rows(cur, work_db=args.work_db, row_ids=[int(row["legacy_id"]) for row in rows])
            conn.commit()
        else:
            conn.rollback()
        cur.close()
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", "candidates": len(rows), "deleted": deleted, "rows": rows}, ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
