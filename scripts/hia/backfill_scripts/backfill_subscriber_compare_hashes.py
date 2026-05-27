# -*- coding: utf-8 -*-
"""
============================================================
Module : backfill_subscriber_compare_hashes.py
Path   : scripts/hia/backfill_scripts/backfill_subscriber_compare_hashes.py
Project: PHR

Purpose:
    Backfill compare hash columns for subscribers.

Responsibility:
    - rebuild compare_identity_norm_hash
    - rebuild compare_other_hash
    - update subscribers table in batches
    - support dry-run verification

Non-goals:
    - subscriber_addresses backfill
    - subscriber_contact_points backfill
    - HIA staging import
    - apply orchestration

Notes:
    compare hash columns are apply-time comparison helpers.

    compare_identity_norm_hash:
        normalized identity comparison hash

    compare_other_hash:
        insured / qualification / organization comparison hash

    This script recalculates hashes from current subscribers values.
============================================================
"""

from __future__ import annotations

import argparse
import hashlib
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


def _sha256_text(parts: list[str]) -> str:
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# ============================================================
# hash builders
# ============================================================


def build_compare_identity_norm_hash(row: dict[str, Any]) -> str:
    """subscribers row から compare_identity_norm_hash を生成する。"""

    return _sha256_text(
        [
            _as_text(row.get("insurer_number")),
            _as_text(row.get("insurance_symbol_digits")),
            _as_text(row.get("insurance_number")),
            _as_text(row.get("birth")),
            _as_text(row.get("gender_code")),
            _as_text(row.get("name_kana_full_match")),
        ]
    )


def build_compare_other_hash(row: dict[str, Any]) -> str:
    """subscribers row から compare_other_hash を生成する。"""

    return _sha256_text(
        [
            _as_text(row.get("insured_attribute_name")),
            _as_text(row.get("relationship_name")),
            _as_text(row.get("qualification_acquired_date")),
            _as_text(row.get("qualification_lost_date")),
            _as_text(row.get("employer_code")),
            _as_text(row.get("department_code")),
            _as_text(row.get("distribution_code")),
            _as_text(row.get("employee_code")),
            _as_text(row.get("connect_id")),
        ]
    )


# ============================================================
# main process
# ============================================================


def backfill_subscriber_compare_hashes(
    *,
    limit: int,
    dry_run: bool,
) -> BackfillMetrics:
    metrics = BackfillMetrics()
    params = load_mysql_base_params()

    with connect_ctx(params, database=DEV_PHR) as conn:
        with dict_cursor(conn) as cur:
            sql = """
            SELECT
                id,
                insurer_number,
                insurance_symbol_digits,
                insurance_number,
                birth,
                gender_code,
                name_kana_full_match,
                insured_attribute_name,
                relationship_name,
                qualification_acquired_date,
                qualification_lost_date,
                employer_code,
                department_code,
                distribution_code,
                employee_code,
                connect_id,
                compare_identity_norm_hash,
                compare_other_hash
            FROM subscribers
            ORDER BY id
            """

            if limit > 0:
                sql += " LIMIT %(limit)s"
                cur.execute(sql, {"limit": limit})
            else:
                cur.execute(sql)

            rows = cast(list[dict[str, Any]], cur.fetchall())

            for row in rows:
                metrics.scanned += 1

                subscriber_id = int(_as_text(row.get("id")) or "0")

                new_identity_hash = build_compare_identity_norm_hash(row)
                new_other_hash = build_compare_other_hash(row)

                current_identity_hash = _as_text(
                    row.get("compare_identity_norm_hash")
                )
                current_other_hash = _as_text(row.get("compare_other_hash"))

                if (
                    current_identity_hash == new_identity_hash
                    and current_other_hash == new_other_hash
                ):
                    metrics.skipped += 1
                    continue

                print(
                    f"[UPDATE] subscriber_id={subscriber_id} "
                    f"identity_changed={current_identity_hash != new_identity_hash} "
                    f"other_changed={current_other_hash != new_other_hash}"
                )

                if not dry_run:
                    cur.execute(
                        """
                        UPDATE subscribers
                        SET
                            compare_identity_norm_hash = %(compare_identity_norm_hash)s,
                            compare_other_hash = %(compare_other_hash)s,
                            updated_at = NOW()
                        WHERE id = %(subscriber_id)s
                        """,
                        {
                            "subscriber_id": subscriber_id,
                            "compare_identity_norm_hash": new_identity_hash,
                            "compare_other_hash": new_other_hash,
                        },
                    )

                metrics.updated += 1

            if dry_run:
                conn.rollback()
            else:
                conn.commit()

    return metrics


# ============================================================
# args
# ============================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill compare hash columns for subscribers."
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

    metrics = backfill_subscriber_compare_hashes(
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print("\n=== backfill_subscriber_compare_hashes ===")
    print(f"scanned : {metrics.scanned}")
    print(f"updated : {metrics.updated}")
    print(f"skipped : {metrics.skipped}")
    print(f"dry_run : {args.dry_run}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())