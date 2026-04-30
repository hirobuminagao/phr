

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
# VSCode の Run ボタン / 直接実行でも `scripts.*` import を解決できるようにする
if __package__ in (None, ""):
    THIS_FILE = Path(__file__).resolve()
    REPO_ROOT = THIS_FILE.parents[3]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.identity.field.address import (
    build_address_match,
    build_postal_code_match,
)

BATCH_SIZE = 1000


@dataclass(frozen=True)
class BackfillSummary:
    scanned: int
    updated: int
    skipped: int
    dry_run: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="staging_subscribers_fund の住所・郵便番号match列をbackfillする",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="処理件数の上限。未指定なら全件対象",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB更新せず件数だけ確認する",
    )
    return parser.parse_args()


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _is_same(left: Any, right: Any) -> bool:
    return _norm_text(left) == _norm_text(right)


def _fetch_target_rows(conn: Any, *, limit: int | None) -> list[dict[str, Any]]:
    cursor = dict_cursor(conn)
    try:
        limit_sql = "" if limit is None else "LIMIT %s"
        params: tuple[Any, ...] = () if limit is None else (limit,)
        cursor.execute(
            f"""
            SELECT
              id,
              postal_code_norm,
              postal_code_match,
              address_line_norm,
              building_norm,
              address_match
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE
              postal_code_norm IS NOT NULL
              OR address_line_norm IS NOT NULL
              OR building_norm IS NOT NULL
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
    postal_code_match = build_postal_code_match(row.get("postal_code_norm"))
    address_match = build_address_match(
        row.get("address_line_norm"),
        row.get("building_norm"),
    )

    update_values: dict[str, Any] = {}

    # 既存値がある場合も、7桁0埋めルールに統一する。
    if not _is_same(row.get("postal_code_match"), postal_code_match):
        update_values["postal_code_match"] = postal_code_match

    # 住所は address_line_norm + 全角スペース + building_norm を比較用形式として統一する。
    if not _is_same(row.get("address_match"), address_match):
        update_values["address_match"] = address_match

    return update_values


def _update_row(conn: Any, *, row_id: int, values: dict[str, Any]) -> None:
    if not values:
        return

    set_clause = ", ".join([f"{column} = %s" for column in values.keys()])
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

    summary = BackfillSummary(
        scanned=scanned,
        updated=updated,
        skipped=skipped,
        dry_run=dry_run,
    )
    print(summary)
    return summary


def main() -> None:
    args = parse_args()
    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()