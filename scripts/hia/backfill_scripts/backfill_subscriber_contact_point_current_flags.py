

# -*- coding: utf-8 -*-
"""
============================================================
Module : backfill_subscriber_contact_point_current_flags.py
Path   : scripts/hia/backfill_scripts/backfill_subscriber_contact_point_current_flags.py
Project: PHR

Purpose:
    Backfill subscriber_contact_points from legacy subscriber_contacts.

Responsibility:
    - read legacy subscriber_contacts current values
    - split phone / email into subscriber_contact_points rows
    - set current rows per subscriber_id + contact_type
    - preserve existing matching contact point rows when possible
    - support dry-run verification

Non-goals:
    - subscribers compare hash backfill
    - subscriber_addresses address_hash backfill
    - HIA staging import
    - apply orchestration
    - subscriber_audit insert

Notes:
    subscriber_contacts is treated as legacy source for this backfill.

    subscriber_contact_points is the target current/history table:

        subscriber_id
        contact_type = phone / email
        contact_value
        is_current
        valid_from
        valid_to

    Backfill rule:

        legacy phone/email blank
            -> clear current contact point for that type

        legacy phone/email present and same row exists
            -> switch that row to current

        legacy phone/email present and no row exists
            -> insert new current row

    This script does not write subscriber_audit.
    It is a migration/backfill utility, not HIA apply.
============================================================
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    phone_inserted: int = 0
    phone_switched: int = 0
    phone_cleared: int = 0
    phone_skipped: int = 0
    email_inserted: int = 0
    email_switched: int = 0
    email_cleared: int = 0
    email_skipped: int = 0


# ============================================================
# helpers
# ============================================================


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _legacy_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _as_text(row.get(key))
        if value:
            return value
    return ""


# ============================================================
# legacy loader
# ============================================================


def load_legacy_contacts(
    cur,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """legacy subscriber_contacts から backfill 元データを取得する。"""

    sql = """
    SELECT
        subscriber_id,
        phone,
        email
    FROM subscriber_contacts
    ORDER BY subscriber_id
    """

    if limit > 0:
        sql += " LIMIT %(limit)s"
        cur.execute(sql, {"limit": limit})
    else:
        cur.execute(sql)

    return list(cur.fetchall())


# ============================================================
# contact point apply helpers
# ============================================================


def clear_current_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
) -> bool:
    """対象 subscriber/contact_type の current を外す。"""

    cur.execute(
        """
        UPDATE subscriber_contact_points
        SET
            is_current = 0,
            valid_to = NOW(),
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND is_current = 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
        },
    )

    return cur.rowcount > 0


def find_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
) -> dict[str, Any] | None:
    """同じ contact_type/contact_value の既存行を探す。"""

    cur.execute(
        """
        SELECT
            contact_point_id,
            is_current
        FROM subscriber_contact_points
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND contact_value = %(contact_value)s
        ORDER BY is_current DESC, contact_point_id DESC
        LIMIT 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": contact_value,
        },
    )

    row = cur.fetchone()
    if not row:
        return None

    return dict(row)


def switch_current_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
) -> None:
    """既存 contact point row を current にする。"""

    clear_current_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
    )

    cur.execute(
        """
        UPDATE subscriber_contact_points
        SET
            is_current = 1,
            valid_from = NOW(),
            valid_to = NULL,
            updated_at = NOW()
        WHERE subscriber_id = %(subscriber_id)s
          AND contact_type = %(contact_type)s
          AND contact_value = %(contact_value)s
        ORDER BY contact_point_id DESC
        LIMIT 1
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": contact_value,
        },
    )

    if cur.rowcount == 0:
        raise RuntimeError(
            "subscriber_contact_points switch affected 0 rows: "
            f"subscriber_id={subscriber_id}, contact_type={contact_type}, "
            f"contact_value={contact_value}"
        )


def insert_contact_point(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
    source: str = "legacy_backfill",
) -> None:
    """subscriber_contact_points に新しい current row を追加する。"""

    clear_current_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
    )

    cur.execute(
        """
        INSERT INTO subscriber_contact_points (
            subscriber_id,
            contact_type,
            contact_value,
            is_current,
            valid_from,
            valid_to,
            source,
            created_at,
            updated_at
        )
        VALUES (
            %(subscriber_id)s,
            %(contact_type)s,
            %(contact_value)s,
            1,
            NOW(),
            NULL,
            %(source)s,
            NOW(),
            NOW()
        )
        """,
        {
            "subscriber_id": subscriber_id,
            "contact_type": contact_type,
            "contact_value": contact_value,
            "source": source,
        },
    )


def backfill_one_contact_type(
    cur,
    *,
    subscriber_id: int,
    contact_type: str,
    contact_value: str,
) -> str:
    """
    1 contact_type の current を legacy 値へ合わせる。

    Returns:
        inserted / switched / cleared / skipped
    """

    value = _as_text(contact_value)

    if not value:
        cleared = clear_current_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
        )
        return "cleared" if cleared else "skipped"

    existing = find_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        contact_value=value,
    )

    if existing and int(existing.get("is_current") or 0) == 1:
        return "skipped"

    if existing:
        switch_current_contact_point(
            cur,
            subscriber_id=subscriber_id,
            contact_type=contact_type,
            contact_value=value,
        )
        return "switched"

    insert_contact_point(
        cur,
        subscriber_id=subscriber_id,
        contact_type=contact_type,
        contact_value=value,
    )
    return "inserted"


# ============================================================
# main process
# ============================================================


def backfill_subscriber_contact_points(
    *,
    limit: int,
    dry_run: bool,
) -> BackfillMetrics:
    metrics = BackfillMetrics()

    params = load_mysql_base_params()

    with connect_ctx(params, database=DEV_PHR) as conn:
        with dict_cursor(conn) as cur:
            try:
                rows = load_legacy_contacts(cur, limit=limit)

                for row in rows:
                    metrics.scanned += 1

                    subscriber_id = int(row["subscriber_id"])
                    phone_value = _legacy_value(row, "phone")
                    email_value = _legacy_value(row, "email")

                    phone_result = backfill_one_contact_type(
                        cur,
                        subscriber_id=subscriber_id,
                        contact_type="phone",
                        contact_value=phone_value,
                    )
                    email_result = backfill_one_contact_type(
                        cur,
                        subscriber_id=subscriber_id,
                        contact_type="email",
                        contact_value=email_value,
                    )

                    if phone_result == "inserted":
                        metrics.phone_inserted += 1
                    elif phone_result == "switched":
                        metrics.phone_switched += 1
                    elif phone_result == "cleared":
                        metrics.phone_cleared += 1
                    else:
                        metrics.phone_skipped += 1

                    if email_result == "inserted":
                        metrics.email_inserted += 1
                    elif email_result == "switched":
                        metrics.email_switched += 1
                    elif email_result == "cleared":
                        metrics.email_cleared += 1
                    else:
                        metrics.email_skipped += 1

                    if phone_result != "skipped" or email_result != "skipped":
                        print(
                            f"[UPDATE] subscriber_id={subscriber_id} "
                            f"phone={phone_result} email={email_result}"
                        )

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
        description=(
            "Backfill subscriber_contact_points current rows "
            "from legacy subscriber_contacts."
        )
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
        help="UPDATE/INSERTをcommitせず rollbackする。",
    )

    return parser.parse_args()


# ============================================================
# main
# ============================================================


def main() -> int:
    args = parse_args()

    metrics = backfill_subscriber_contact_points(
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print("\n=== backfill_subscriber_contact_point_current_flags ===")
    print(f"scanned        : {metrics.scanned}")
    print(f"phone_inserted : {metrics.phone_inserted}")
    print(f"phone_switched : {metrics.phone_switched}")
    print(f"phone_cleared  : {metrics.phone_cleared}")
    print(f"phone_skipped  : {metrics.phone_skipped}")
    print(f"email_inserted : {metrics.email_inserted}")
    print(f"email_switched : {metrics.email_switched}")
    print(f"email_cleared  : {metrics.email_cleared}")
    print(f"email_skipped  : {metrics.email_skipped}")
    print(f"dry_run        : {args.dry_run}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())