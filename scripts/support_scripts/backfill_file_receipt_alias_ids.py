#!/usr/bin/env python3
"""Backfill file_receipts.medical_folder_alias_id from exact first-folder matches."""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor


def qname(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"invalid schema name: {value!r}")
    return f"`{value}`"


def first_folder(relative_path: Any) -> str:
    normalized = str(relative_path or "").replace("\\", "/").lstrip("/")
    parts = PurePosixPath(normalized).parts
    return parts[0] if parts else ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default="health_exam_result")
    parser.add_argument("--master-db", default="phr_master")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.health_db, autocommit=False) as conn:
        cur = dict_cursor(conn)
        alias_params: list[Any] = []
        alias_where = ""
        if args.event_id is not None:
            alias_where = "WHERE event_id = %s"
            alias_params.append(args.event_id)
        cur.execute(
            f"SELECT alias_id, event_id, src_folder_raw FROM {qname(args.master_db)}.medical_folder_aliases {alias_where}",
            tuple(alias_params),
        )
        aliases: dict[tuple[int, str], list[int]] = defaultdict(list)
        for row in cur.fetchall():
            aliases[(int(row["event_id"]), str(row["src_folder_raw"]))].append(int(row["alias_id"]))

        receipt_params: list[Any] = []
        receipt_where = "medical_folder_alias_id IS NULL"
        if args.event_id is not None:
            receipt_where += " AND event_id = %s"
            receipt_params.append(args.event_id)
        cur.execute(
            f"SELECT id, event_id, relative_path FROM {qname(args.health_db)}.file_receipts WHERE {receipt_where} ORDER BY id",
            tuple(receipt_params),
        )
        matched: list[tuple[int, int]] = []
        unresolved: list[dict[str, Any]] = []
        for row in cur.fetchall():
            folder = first_folder(row.get("relative_path"))
            candidates = aliases.get((int(row["event_id"]), folder), [])
            if len(candidates) == 1:
                matched.append((candidates[0], int(row["id"])))
            else:
                unresolved.append({"id": row["id"], "event_id": row["event_id"], "folder": folder, "candidates": candidates})

        print(f"mode={'apply' if args.apply else 'dry-run'} matched={len(matched)} unresolved={len(unresolved)}")
        for row in unresolved[:100]:
            print(f"[UNRESOLVED] file_receipt_id={row['id']} event_id={row['event_id']} folder={row['folder']!r} candidates={row['candidates']}")
        if args.apply and matched:
            cur.executemany(
                f"UPDATE {qname(args.health_db)}.file_receipts SET medical_folder_alias_id = %s WHERE id = %s AND medical_folder_alias_id IS NULL",
                matched,
            )
            conn.commit()
            print(f"updated={cur.rowcount}")
        else:
            conn.rollback()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
