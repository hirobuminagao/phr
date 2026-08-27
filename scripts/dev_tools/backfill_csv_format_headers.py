#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill csv_format_versions header snapshots from local sample CSV files."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.csv_mapping_lab.analyze_csv import normalize_header
from scripts.lib.csv.exam_result_format_matcher import load_csv_for_format
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor


DEFAULT_SAMPLE_DIRS = (
    REPO_ROOT / "docs" / "spec" / "exam_result_csv_import" / "samples",
    REPO_ROOT / "tests" / "fixtures" / "from_medical_event2",
)


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def csv_header_snapshot_for_result(csv_result: Any) -> dict[str, Any]:
    return {
        "active_header_row_no": csv_result.header_set.active_header_row_no,
        "header_rows": csv_result.header_set.header_rows,
        "normalized_columns": [
            {
                "column_no": column.column_no,
                "context": column.context,
                "header_name": column.header_name,
                "occurrence": column.occurrence,
            }
            for column in csv_result.header_set.normalized_columns
        ],
    }


def table_exists(cur: Any, *, schema_name: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema_name, table_name),
    )
    row = cur.fetchone()
    return bool(row and int(row.get("cnt") or 0) > 0)


def discover_csv_files(sample_dirs: list[Path]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for directory in sample_dirs:
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.csv")):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(path)
    return files


def load_formats(cur: Any, *, master_db: str, only_missing: bool) -> list[dict[str, Any]]:
    where = ""
    if only_missing:
        where = "WHERE cfv.`header_snapshot_json` IS NULL OR h.`header_count` = 0"
    cur.execute(
        f"""
        SELECT
          cfv.*,
          COALESCE(h.`header_count`, 0) AS `header_column_count`
        FROM {qname(master_db)}.`csv_format_versions` AS cfv
        LEFT JOIN (
          SELECT `csv_format_version_id`, COUNT(*) AS `header_count`
          FROM {qname(master_db)}.`csv_format_header_columns`
          GROUP BY `csv_format_version_id`
        ) AS h
          ON h.`csv_format_version_id` = cfv.`csv_format_version_id`
        {where}
        ORDER BY cfv.`csv_format_version_id`
        """
    )
    return [dict(row) for row in cur.fetchall()]


def save_header(cur: Any, *, master_db: str, fmt: dict[str, Any], csv_result: Any) -> None:
    csv_format_version_id = int(fmt["csv_format_version_id"])
    snapshot = csv_header_snapshot_for_result(csv_result)
    cur.execute(
        f"""
        UPDATE {qname(master_db)}.`csv_format_versions`
           SET `header_sha256` = %s,
               `header_snapshot_json` = %s,
               `header_hash_status` = 'VERIFIED'
         WHERE `csv_format_version_id` = %s
        """,
        (
            csv_result.header_set.header_sha256,
            json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
            csv_format_version_id,
        ),
    )
    cur.execute(
        f"""
        DELETE FROM {qname(master_db)}.`csv_format_header_columns`
        WHERE `csv_format_version_id` = %s
        """,
        (csv_format_version_id,),
    )
    rows = [
        (
            csv_format_version_id,
            column.column_no,
            column.context,
            column.header_name,
            normalize_header(column.header_name),
            column.occurrence,
        )
        for column in csv_result.header_set.normalized_columns
    ]
    if rows:
        cur.executemany(
            f"""
            INSERT INTO {qname(master_db)}.`csv_format_header_columns` (
              `csv_format_version_id`,
              `column_no`,
              `header_context`,
              `header_name`,
              `normalized_header_name`,
              `header_occurrence`
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def sample_score(fmt: dict[str, Any], path: Path) -> int:
    haystack = " ".join(
        [
            str(path).lower(),
            path.stem.lower(),
            path.parent.name.lower(),
        ]
    )
    mapping_version = str(fmt.get("mapping_version") or "").lower()
    format_name = str(fmt.get("format_name") or "").lower()
    score = 0
    for token in re_split_tokens(mapping_version):
        if len(token) >= 4 and token in haystack:
            score += 5
    for token in re_split_tokens(format_name):
        if len(token) >= 3 and token in haystack:
            score += 2
    return score


def re_split_tokens(value: str) -> list[str]:
    return [token for token in re.split(r"[^0-9a-zA-Zぁ-んァ-ン一-龥]+", value) if token]


def find_matching_sample(fmt: dict[str, Any], csv_files: list[Path]) -> tuple[Path, Any] | None:
    expected_sha = str(fmt.get("header_sha256") or "").strip()
    if not expected_sha:
        return None
    matches: list[tuple[int, Path, Any]] = []
    for path in csv_files:
        try:
            csv_result = load_csv_for_format(str(path), fmt)
        except Exception:
            continue
        if csv_result.header_set.header_sha256 == expected_sha:
            matches.append((sample_score(fmt, path), path, csv_result))
    if not matches:
        return None
    matches.sort(key=lambda item: (-item[0], str(item[1])))
    _, path, csv_result = matches[0]
    return path, csv_result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-prefix", default="PHR_DB_", help="Environment variable prefix. default: PHR_DB_")
    parser.add_argument("--master-db", default="phr_master")
    parser.add_argument("--all", action="store_true", help="Rebuild headers even when already present.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--sample-dir",
        action="append",
        type=Path,
        help="Directory to search recursively for CSV samples. Can be specified multiple times.",
    )
    args = parser.parse_args()

    sample_dirs = args.sample_dir or list(DEFAULT_SAMPLE_DIRS)
    csv_files = discover_csv_files(sample_dirs)
    if not csv_files:
        print("No sample CSV files found.")
        return 1

    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.master_db, autocommit=False) as conn:
        cur = dict_cursor(conn)
        if not table_exists(cur, schema_name=args.master_db, table_name="csv_format_header_columns"):
            raise RuntimeError("csv_format_header_columns does not exist. Apply migration first.")

        formats = load_formats(cur, master_db=args.master_db, only_missing=not args.all)
        restored = 0
        missing: list[str] = []
        for fmt in formats:
            match = find_matching_sample(fmt, csv_files)
            if not match:
                missing.append(f"{fmt.get('csv_format_version_id')}:{fmt.get('mapping_version')}")
                continue
            path, csv_result = match
            restored += 1
            print(
                "MATCH",
                fmt.get("csv_format_version_id"),
                fmt.get("mapping_version"),
                f"columns={len(csv_result.header_set.normalized_columns)}",
                f"sample={path}",
            )
            if not args.dry_run:
                save_header(cur, master_db=args.master_db, fmt=fmt, csv_result=csv_result)

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(f"restored={restored} missing={len(missing)} dry_run={args.dry_run}")
    for item in missing:
        print("MISSING", item)
    return 0 if restored or not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
