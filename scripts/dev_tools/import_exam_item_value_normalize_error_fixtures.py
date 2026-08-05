#!/usr/bin/env python3
"""Import anonymized exam_item_values normalize error CSV fixtures.

This is a development analysis tool. It imports grouped, non-personal error
rows exported from an execution environment for norm_variants review and
regression checks.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx


DEFAULT_SOURCE = PROJECT_ROOT / "scripts" / "dev_tools" / "import_csv" / "exam_item_values_error_20260805.csv"
DEFAULT_HEALTH_DB = "health_exam_result"
DEFAULT_SOURCE_LABEL = "exam_item_values_error_20260805"
EXPECTED_HEADER = [
    "namecode",
    "namecode_display_name",
    "raw_value",
    "raw_value_type",
    "code_system",
    "normalize_status",
    "normalize_reason",
    "validation_status",
    "validation_reason",
    "cnt",
]


@dataclass(frozen=True)
class NormalizeErrorFixture:
    source_label: str
    source_file_name: str
    source_file_sha256: str
    source_row_no: int
    source_row_sha256: str
    namecode: str
    namecode_display_name: str | None
    raw_value: str | None
    raw_value_type: str | None
    code_system: str | None
    normalize_status: str
    normalize_reason: str | None
    validation_status: str
    validation_reason: str | None
    sample_count: int


@dataclass
class ImportSummary:
    source: Path
    source_label: str
    rows_read: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    total_sample_count: int = 0
    inserted_or_updated: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import grouped exam_item_values normalize errors into health_exam_result."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--source-label", default=DEFAULT_SOURCE_LABEL)
    parser.add_argument("--health-db", default=DEFAULT_HEALTH_DB)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--apply", action="store_true", help="Write to DB. Without this option, only validates and summarizes.")
    parser.add_argument("--replace", action="store_true", help="Delete existing rows for source-label before insert. Requires --apply.")
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid identifier: {name}")
    return f"`{name}`"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def row_sha256(row: dict[str, str]) -> str:
    joined = "\x1f".join(row.get(column, "") for column in EXPECTED_HEADER)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def parse_count(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ValueError(f"invalid cnt: {value!r}")
    return int(text)


def parse_row(
    row: dict[str, str],
    *,
    source_label: str,
    source_file_name: str,
    source_file_sha256: str,
    source_row_no: int,
) -> NormalizeErrorFixture:
    namecode = row["namecode"].strip()
    if not re.fullmatch(r"[0-9A-Z]{17}", namecode):
        raise ValueError(f"invalid namecode: {namecode!r}")

    normalize_status = row["normalize_status"].strip()
    validation_status = row["validation_status"].strip()
    if not normalize_status:
        raise ValueError("normalize_status is required")
    if not validation_status:
        raise ValueError("validation_status is required")

    return NormalizeErrorFixture(
        source_label=source_label,
        source_file_name=source_file_name,
        source_file_sha256=source_file_sha256,
        source_row_no=source_row_no,
        source_row_sha256=row_sha256(row),
        namecode=namecode,
        namecode_display_name=optional_text(row.get("namecode_display_name")),
        raw_value=optional_text(row.get("raw_value")),
        raw_value_type=optional_text(row.get("raw_value_type")),
        code_system=optional_text(row.get("code_system")),
        normalize_status=normalize_status,
        normalize_reason=optional_text(row.get("normalize_reason")),
        validation_status=validation_status,
        validation_reason=optional_text(row.get("validation_reason")),
        sample_count=parse_count(row["cnt"]),
    )


def read_source(path: Path, *, source_label: str) -> tuple[list[NormalizeErrorFixture], ImportSummary]:
    source_file_sha256 = file_sha256(path)
    summary = ImportSummary(source=path, source_label=source_label)
    rows: list[NormalizeErrorFixture] = []

    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        if reader.fieldnames != EXPECTED_HEADER:
            raise ValueError(f"unexpected header: {reader.fieldnames!r}")

        for source_row_no, raw_row in enumerate(reader, start=2):
            summary.rows_read += 1
            try:
                row = parse_row(
                    raw_row,
                    source_label=source_label,
                    source_file_name=path.name,
                    source_file_sha256=source_file_sha256,
                    source_row_no=source_row_no,
                )
            except Exception:
                summary.rows_invalid += 1
                continue

            summary.rows_valid += 1
            summary.total_sample_count += row.sample_count
            rows.append(row)

    return rows, summary


def chunks(values: list[NormalizeErrorFixture], size: int) -> Iterable[list[NormalizeErrorFixture]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def row_params(row: NormalizeErrorFixture) -> tuple[Any, ...]:
    return (
        row.source_label,
        row.source_file_name,
        row.source_file_sha256,
        row.source_row_no,
        row.source_row_sha256,
        row.namecode,
        row.namecode_display_name,
        row.raw_value,
        row.raw_value_type,
        row.code_system,
        row.normalize_status,
        row.normalize_reason,
        row.validation_status,
        row.validation_reason,
        row.sample_count,
    )


def upsert_rows(cur: Any, health_db: str, rows: list[NormalizeErrorFixture], batch_size: int) -> int:
    columns = [
        "source_label",
        "source_file_name",
        "source_file_sha256",
        "source_row_no",
        "source_row_sha256",
        "namecode",
        "namecode_display_name",
        "raw_value",
        "raw_value_type",
        "code_system",
        "normalize_status",
        "normalize_reason",
        "validation_status",
        "validation_reason",
        "sample_count",
    ]
    column_sql = ", ".join(qname(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f"{qname(column)} = VALUES({qname(column)})"
        for column in columns
        if column not in {"source_label", "source_row_sha256"}
    )
    sql = f"""
        INSERT INTO {qname(health_db)}.`exam_item_value_normalize_error_fixtures` ({column_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          {updates},
          `updated_at` = CURRENT_TIMESTAMP(3)
    """

    total = 0
    for batch in chunks(rows, batch_size):
        cur.executemany(sql, [row_params(row) for row in batch])
        total += len(batch)
    return total


def print_summary(summary: ImportSummary, *, applied: bool, replaced: bool) -> None:
    print(f"source={summary.source}")
    print(f"source_label={summary.source_label}")
    print(f"rows_read={summary.rows_read}")
    print(f"rows_valid={summary.rows_valid}")
    print(f"rows_invalid={summary.rows_invalid}")
    print(f"total_sample_count={summary.total_sample_count}")
    print(f"applied={int(applied)}")
    print(f"replace={int(replaced)}")
    print(f"inserted_or_updated={summary.inserted_or_updated}")


def main() -> int:
    args = parse_args()
    if args.replace and not args.apply:
        raise SystemExit("--replace requires --apply")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if not args.source.exists():
        raise SystemExit(f"source not found: {args.source}")

    rows, summary = read_source(args.source, source_label=args.source_label)
    if summary.rows_invalid:
        print_summary(summary, applied=False, replaced=False)
        raise SystemExit("invalid rows found")

    if not args.apply:
        print_summary(summary, applied=False, replaced=False)
        return 0

    params = load_mysql_base_params()
    with connect_ctx(params, database=args.health_db, autocommit=False) as conn:
        cur = conn.cursor()
        try:
            if args.replace:
                cur.execute(
                    f"""
                    DELETE FROM {qname(args.health_db)}.`exam_item_value_normalize_error_fixtures`
                    WHERE `source_label` = %s
                    """,
                    (args.source_label,),
                )
            summary.inserted_or_updated = upsert_rows(cur, args.health_db, rows, args.batch_size)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    print_summary(summary, applied=True, replaced=bool(args.replace))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
