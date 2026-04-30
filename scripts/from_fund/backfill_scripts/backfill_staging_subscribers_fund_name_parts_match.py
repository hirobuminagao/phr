

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

# ------------------------------------------------------------
# sys.path bootstrap
# ------------------------------------------------------------
if __package__ in (None, ""):
    THIS_FILE = Path(__file__).resolve()
    REPO_ROOT = THIS_FILE.parents[3]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.identity.field import name_kana

BATCH_SIZE = 1000


@dataclass(frozen=True)
class BackfillSummary:
    scanned: int
    updated: int
    skipped: int
    dry_run: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="staging_subscribers_fund のカナparts match列をnormからbackfillする",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _is_blank(value: Any) -> bool:
    return _norm_text(value) is None


def _fetch_target_rows(conn: Any, *, limit: int | None) -> list[dict[str, Any]]:
    cursor = dict_cursor(conn)
    try:
        limit_sql = "" if limit is None else "LIMIT %s"
        params: tuple[Any, ...] = () if limit is None else (limit,)
        cursor.execute(
            f"""
            SELECT
              id,
              name_kana_family_norm,
              name_kana_middle_norm,
              name_kana_given_norm,
              name_kana_family_match,
              name_kana_middle_match,
              name_kana_given_match
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE
              (name_kana_family_norm IS NOT NULL
               OR name_kana_middle_norm IS NOT NULL
               OR name_kana_given_norm IS NOT NULL)
              AND
              (name_kana_family_match IS NULL
               OR name_kana_middle_match IS NULL
               OR name_kana_given_match IS NULL)
            ORDER BY id
            {limit_sql}
            """,
            params,
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def _build_update(row: dict[str, Any]) -> dict[str, Any]:
    parts = {
        "family": _norm_text(row.get("name_kana_family_norm")),
        "middle": _norm_text(row.get("name_kana_middle_norm")),
        "given": _norm_text(row.get("name_kana_given_norm")),
    }

    if not any(parts.values()):
        return {}

    match_parts = name_kana.norm_parts_to_match_parts(parts)

    update_values: dict[str, Any] = {}

    if _is_blank(row.get("name_kana_family_match")):
        update_values["name_kana_family_match"] = match_parts.get("family")
    if _is_blank(row.get("name_kana_middle_match")):
        update_values["name_kana_middle_match"] = match_parts.get("middle")
    if _is_blank(row.get("name_kana_given_match")):
        update_values["name_kana_given_match"] = match_parts.get("given")

    return update_values


def _update_row(conn: Any, *, row_id: int, values: dict[str, Any]) -> None:
    if not values:
        return

    set_clause = ", ".join([f"{k} = %s" for k in values.keys()])
    params = tuple(values.values()) + (row_id,)

    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            UPDATE {DEV_PHR}.staging_subscribers_fund
            SET {set_clause}
            WHERE id = %s
            """,
            params,
        )
    finally:
        cursor.close()


def run(*, limit: int | None = None, dry_run: bool = False) -> BackfillSummary:
    params = load_mysql_base_params()
    scanned = 0
    updated = 0
    skipped = 0

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        rows = _fetch_target_rows(conn, limit=limit)

        for row in rows:
            scanned += 1
            update_values = _build_update(row)

            if not update_values:
                skipped += 1
                continue

            updated += 1

            if not dry_run:
                _update_row(conn, row_id=int(row["id"]), values=update_values)

            if updated % BATCH_SIZE == 0:
                print(f"[INFO] updated={updated} scanned={scanned}")

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    summary = BackfillSummary(scanned, updated, skipped, dry_run)
    print(summary)
    return summary


def main() -> None:
    args = parse_args()
    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()