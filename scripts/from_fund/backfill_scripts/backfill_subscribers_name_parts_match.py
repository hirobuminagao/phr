

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
from scripts.lib.identity.field import name_kana, name_kanji

BATCH_SIZE = 1000


@dataclass(frozen=True)
class BackfillSummary:
    scanned: int
    updated: int
    skipped: int
    dry_run: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="subscribers の氏名parts match列を既存parts値からbackfillする",
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
              name_kana_family,
              name_kana_middle,
              name_kana_given,
              name_kana_family_match,
              name_kana_middle_match,
              name_kana_given_match,
              name_kanji_family,
              name_kanji_middle,
              name_kanji_given,
              name_kanji_family_match,
              name_kanji_middle_match,
              name_kanji_given_match
            FROM {DEV_PHR}.subscribers
            WHERE
              (
                (name_kana_family IS NOT NULL OR name_kana_middle IS NOT NULL OR name_kana_given IS NOT NULL)
                AND (name_kana_family_match IS NULL OR name_kana_middle_match IS NULL OR name_kana_given_match IS NULL)
              )
              OR
              (
                (name_kanji_family IS NOT NULL OR name_kanji_middle IS NOT NULL OR name_kanji_given IS NOT NULL)
                AND (name_kanji_family_match IS NULL OR name_kanji_middle_match IS NULL OR name_kanji_given_match IS NULL)
              )
            ORDER BY id
            {limit_sql}
            """,
            params,
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]


def _build_match_update(row: dict[str, Any]) -> dict[str, Any]:
    update_values: dict[str, Any] = {}

    kana_parts = {
        "family": _norm_text(row.get("name_kana_family")),
        "middle": _norm_text(row.get("name_kana_middle")),
        "given": _norm_text(row.get("name_kana_given")),
    }
    if any(kana_parts.values()):
        kana_match_parts = name_kana.norm_parts_to_match_parts(kana_parts)
        if _is_blank(row.get("name_kana_family_match")):
            update_values["name_kana_family_match"] = kana_match_parts.get("family")
        if _is_blank(row.get("name_kana_middle_match")):
            update_values["name_kana_middle_match"] = kana_match_parts.get("middle")
        if _is_blank(row.get("name_kana_given_match")):
            update_values["name_kana_given_match"] = kana_match_parts.get("given")

    kanji_parts = {
        "family": _norm_text(row.get("name_kanji_family")),
        "middle": _norm_text(row.get("name_kanji_middle")),
        "given": _norm_text(row.get("name_kanji_given")),
    }
    if any(kanji_parts.values()):
        kanji_match_parts = name_kanji.norm_parts_to_match_parts(kanji_parts)
        if _is_blank(row.get("name_kanji_family_match")):
            update_values["name_kanji_family_match"] = kanji_match_parts.get("family")
        if _is_blank(row.get("name_kanji_middle_match")):
            update_values["name_kanji_middle_match"] = kanji_match_parts.get("middle")
        if _is_blank(row.get("name_kanji_given_match")):
            update_values["name_kanji_given_match"] = kanji_match_parts.get("given")

    return update_values


def _update_row(conn: Any, *, subscriber_id: int, values: dict[str, Any]) -> None:
    if not values:
        return

    set_clause = ", ".join([f"{column} = %s" for column in values.keys()])
    params = tuple(values.values()) + (subscriber_id,)

    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            UPDATE {DEV_PHR}.subscribers
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
            update_values = _build_match_update(row)
            if not update_values:
                skipped += 1
                continue

            updated += 1
            if not dry_run:
                _update_row(
                    conn,
                    subscriber_id=int(row["id"]),
                    values=update_values,
                )

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