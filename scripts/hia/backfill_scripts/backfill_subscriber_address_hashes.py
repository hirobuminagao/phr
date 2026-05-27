# -*- coding: utf-8 -*-
"""
============================================================
Module : backfill_subscriber_address_hashes.py
Path   : scripts/hia/backfill_scripts/backfill_subscriber_address_hashes.py
Project: PHR

Purpose:
    Backfill address_hash for subscriber_addresses.

Responsibility:
    - rebuild address_hash from current address values
    - update subscriber_addresses in batches
    - support dry-run verification

Non-goals:
    - subscribers compare hash backfill
    - subscriber_contact_points backfill
    - HIA staging import
    - apply orchestration

Notes:
    address_hash is used for:

        - address compare
        - switch_current detection
        - history/current management

    This script recalculates address_hash from current row values.
============================================================
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

# repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.hash.compare_hash import build_compare_hash


# ============================================================
# metrics
# ============================================================


@dataclass
class BackfillMetrics:
    scanned: int = 0
    updated: int = 0
    skipped: int = 0


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


# ============================================================
# hash builder
# ============================================================


def build_address_hash(row: dict[str, Any]) -> str:
    """subscriber_addresses row から address_hash を生成する。"""

    return build_compare_hash(
        [
            row.get("postal_code"),
            row.get("address_line"),
            row.get("building"),
        ]
    )


# ============================================================
# main process
# ============================================================


def backfill_subscriber_address_hashes(
    *,
    limit: int,
    dry_run: bool,
) -> BackfillMetrics:
    metrics = BackfillMetrics()

    params = load_mysql_base_params()

    with connect_ctx(params, database=DEV_PHR) as conn:
        with dict_cursor(conn) as cur:
            try:
                sql = """
                SELECT
                    address_id,
                    subscriber_id,
                    postal_code,
                    address_line,
                    building,
                    address_hash
                FROM subscriber_addresses
                ORDER BY address_id
                """

                if limit > 0:
                    sql += " LIMIT %(limit)s"
                    cur.execute(sql, {"limit": limit})
                else:
                    cur.execute(sql)

                rows = cast(list[dict[str, Any]], cur.fetchall())

                for row in rows:
                    metrics.scanned += 1

                    address_id = int(
                        _as_text(row.get("address_id")) or "0"
                    )
                    subscriber_id = int(
                        _as_text(row.get("subscriber_id")) or "0"
                    )

                    new_hash = build_address_hash(row)
                    current_hash = _as_text(row.get("address_hash"))

                    if current_hash == new_hash:
                        metrics.skipped += 1
                        continue

                    print(
                        f"[UPDATE] address_id={address_id} "
                        f"subscriber_id={subscriber_id}"
                    )

                    if not dry_run:
                        cur.execute(
                            """
                            UPDATE subscriber_addresses
                            SET
                                address_hash = %(address_hash)s,
                                updated_at = NOW()
                            WHERE address_id = %(address_id)s
                            """,
                            {
                                "address_id": address_id,
                                "address_hash": new_hash,
                            },
                        )

                    metrics.updated += 1

                if dry_run:
                    conn.rollback()
                else:
                    conn.commit()

                return metrics

            finally:
                pass


# ============================================================
# args
# ============================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill address_hash for subscriber_addresses."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="処理件数上限。0 の場合は無制限。",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="UPDATEをcommitせず rollbackする。",
    )

    return parser.parse_args()


# ============================================================
# main
# ============================================================


def main() -> int:
    args = parse_args()

    metrics = backfill_subscriber_address_hashes(
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print("\n=== backfill_subscriber_address_hashes ===")
    print(f"scanned : {metrics.scanned}")
    print(f"updated : {metrics.updated}")
    print(f"skipped : {metrics.skipped}")
    print(f"dry_run : {args.dry_run}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())