#!/usr/bin/env python3
"""Import Japan Post UTF-8 postal code CSV into phr_master.

This is a master-data maintenance tool, not part of the numbered medical
result import flow.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx


DEFAULT_SOURCE = Path("/Users/hiro/Downloads/utf_ken_all.csv")
DEFAULT_MASTER_DB = "phr_master"
DEFAULT_SOURCE_NAME = "日本郵便 住所の郵便番号 1レコード1行 UTF-8"
EXPECTED_COLUMNS = 15
NO_TOWN_AREA = "以下に掲載がない場合"


@dataclass(frozen=True)
class PostalCodeAddress:
    jis_code: str
    old_postal_code: str | None
    postal_code: str
    postal_code_formatted: str
    prefecture_kana: str | None
    city_kana: str | None
    town_area_kana: str | None
    prefecture: str
    city: str
    town_area_raw: str
    town_area_normalized: str
    address_for_xml: str
    is_multi_postal_town: int
    has_koaza_numbering: int
    has_chome: int
    is_multi_town_postal: int
    update_flag: str
    change_reason_code: str
    normalization_note: str | None
    data_source_name: str
    data_source_file_name: str
    data_source_file_sha256: str
    data_source_note: str
    source_file_updated_at: datetime
    source_row_sha256: str


@dataclass
class ImportSummary:
    source: Path
    rows_read: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    no_town_area_rows: int = 0
    next_to_banchi_rows: int = 0
    ichien_rows: int = 0
    duplicate_postal_codes: int = 0
    max_rows_per_postal_code: int = 0
    inserted_or_updated: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Japan Post utf_ken_all.csv into phr_master.postal_code_addresses."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--master-db", default=DEFAULT_MASTER_DB)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--apply", action="store_true", help="Write to DB. Without this option, only validates and summarizes.")
    parser.add_argument("--replace", action="store_true", help="Delete existing rows before insert. Requires --apply.")
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


def row_sha256(row: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update("\x1f".join(row).encode("utf-8"))
    return digest.hexdigest()


def normalize_postal_code(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) != 7:
        raise ValueError(f"invalid postal code: {value!r}")
    return digits


def format_postal_code(postal_code: str) -> str:
    return f"{postal_code[:3]}-{postal_code[3:]}"


def optional_text(value: str) -> str | None:
    text = value.strip()
    return text or None


def flag(value: str) -> int:
    text = value.strip()
    if text not in {"0", "1"}:
        raise ValueError(f"invalid flag: {value!r}")
    return int(text)


def normalize_town_area(town_area_raw: str, city: str) -> tuple[str, str | None]:
    town = town_area_raw.strip()
    if town == NO_TOWN_AREA:
        return "", "町域が「以下に掲載がない場合」のため、市区町村までを補完住所とする。"
    if "の次に番地がくる場合" in town:
        return "", "町域が「の次に番地がくる場合」のため、市区町村までを補完住所とする。"
    if town.endswith("一円") and town.startswith(city):
        return "", "町域が市区町村名と重複する「一円」表記のため、市区町村までを補完住所とする。"
    if "（その他）" in town:
        return town, "町域に「その他」を含むため、候補確認用に特殊表記として保持する。"
    if "（" in town or "）" in town:
        return town, "町域に括弧表記を含むため、候補確認用に特殊表記として保持する。"
    return town, None


def parse_row(
    row: list[str],
    *,
    source_file_name: str,
    source_file_sha256: str,
    source_file_updated_at: datetime,
) -> PostalCodeAddress:
    if len(row) != EXPECTED_COLUMNS:
        raise ValueError(f"expected {EXPECTED_COLUMNS} columns, got {len(row)}")

    postal_code = normalize_postal_code(row[2])
    prefecture = row[6].strip()
    city = row[7].strip()
    town_area_raw = row[8].strip()
    town_area_normalized, note = normalize_town_area(town_area_raw, city)
    address_for_xml = f"{prefecture}{city}{town_area_normalized}"

    return PostalCodeAddress(
        jis_code=row[0].strip(),
        old_postal_code=optional_text(row[1]),
        postal_code=postal_code,
        postal_code_formatted=format_postal_code(postal_code),
        prefecture_kana=optional_text(row[3]),
        city_kana=optional_text(row[4]),
        town_area_kana=optional_text(row[5]),
        prefecture=prefecture,
        city=city,
        town_area_raw=town_area_raw,
        town_area_normalized=town_area_normalized,
        address_for_xml=address_for_xml,
        is_multi_postal_town=flag(row[9]),
        has_koaza_numbering=flag(row[10]),
        has_chome=flag(row[11]),
        is_multi_town_postal=flag(row[12]),
        update_flag=row[13].strip() or "0",
        change_reason_code=row[14].strip() or "0",
        normalization_note=note,
        data_source_name=DEFAULT_SOURCE_NAME,
        data_source_file_name=source_file_name,
        data_source_file_sha256=source_file_sha256,
        data_source_note="日本郵便公開CSV由来。社内作業データ、受領CSV、機微情報を含まない。",
        source_file_updated_at=source_file_updated_at,
        source_row_sha256=row_sha256(row),
    )


def read_source(path: Path) -> tuple[list[PostalCodeAddress], ImportSummary]:
    source_file_sha256 = file_sha256(path)
    source_file_updated_at = datetime.fromtimestamp(path.stat().st_mtime)
    summary = ImportSummary(source=path)
    rows: list[PostalCodeAddress] = []
    postal_counts: dict[str, int] = {}

    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp)
        for raw_row in reader:
            summary.rows_read += 1
            try:
                row = parse_row(
                    raw_row,
                    source_file_name=path.name,
                    source_file_sha256=source_file_sha256,
                    source_file_updated_at=source_file_updated_at,
                )
            except Exception:
                summary.rows_invalid += 1
                continue

            summary.rows_valid += 1
            rows.append(row)
            postal_counts[row.postal_code] = postal_counts.get(row.postal_code, 0) + 1
            if row.town_area_raw == NO_TOWN_AREA:
                summary.no_town_area_rows += 1
            if "の次に番地がくる場合" in row.town_area_raw:
                summary.next_to_banchi_rows += 1
            if row.town_area_raw.endswith("一円"):
                summary.ichien_rows += 1

    summary.duplicate_postal_codes = sum(1 for count in postal_counts.values() if count > 1)
    summary.max_rows_per_postal_code = max(postal_counts.values(), default=0)
    return rows, summary


def chunks(values: list[PostalCodeAddress], size: int) -> Iterable[list[PostalCodeAddress]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def row_params(row: PostalCodeAddress) -> tuple[Any, ...]:
    return (
        row.jis_code,
        row.old_postal_code,
        row.postal_code,
        row.postal_code_formatted,
        row.prefecture_kana,
        row.city_kana,
        row.town_area_kana,
        row.prefecture,
        row.city,
        row.town_area_raw,
        row.town_area_normalized,
        row.address_for_xml,
        row.is_multi_postal_town,
        row.has_koaza_numbering,
        row.has_chome,
        row.is_multi_town_postal,
        row.update_flag,
        row.change_reason_code,
        row.normalization_note,
        row.data_source_name,
        row.data_source_file_name,
        row.data_source_file_sha256,
        row.data_source_note,
        row.source_file_updated_at,
        row.source_row_sha256,
    )


def upsert_rows(cur: Any, master_db: str, rows: list[PostalCodeAddress], batch_size: int) -> int:
    columns = [
        "jis_code",
        "old_postal_code",
        "postal_code",
        "postal_code_formatted",
        "prefecture_kana",
        "city_kana",
        "town_area_kana",
        "prefecture",
        "city",
        "town_area_raw",
        "town_area_normalized",
        "address_for_xml",
        "is_multi_postal_town",
        "has_koaza_numbering",
        "has_chome",
        "is_multi_town_postal",
        "update_flag",
        "change_reason_code",
        "normalization_note",
        "data_source_name",
        "data_source_file_name",
        "data_source_file_sha256",
        "data_source_note",
        "source_file_updated_at",
        "source_row_sha256",
    ]
    column_sql = ", ".join(qname(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(
        f"{qname(column)} = VALUES({qname(column)})"
        for column in columns
        if column != "source_row_sha256"
    )
    sql = f"""
        INSERT INTO {qname(master_db)}.`postal_code_addresses` ({column_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          {updates},
          `is_active` = 1,
          `updated_at` = CURRENT_TIMESTAMP(3)
    """

    total = 0
    for batch in chunks(rows, batch_size):
        cur.executemany(sql, [row_params(row) for row in batch])
        total += len(batch)
    return total


def print_summary(summary: ImportSummary, *, applied: bool, replaced: bool) -> None:
    print(f"source={summary.source}")
    print(f"rows_read={summary.rows_read}")
    print(f"rows_valid={summary.rows_valid}")
    print(f"rows_invalid={summary.rows_invalid}")
    print(f"duplicate_postal_codes={summary.duplicate_postal_codes}")
    print(f"max_rows_per_postal_code={summary.max_rows_per_postal_code}")
    print(f"no_town_area_rows={summary.no_town_area_rows}")
    print(f"next_to_banchi_rows={summary.next_to_banchi_rows}")
    print(f"ichien_rows={summary.ichien_rows}")
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

    rows, summary = read_source(args.source)
    if summary.rows_invalid:
        print_summary(summary, applied=False, replaced=False)
        raise SystemExit("invalid rows found")

    if not args.apply:
        print_summary(summary, applied=False, replaced=False)
        return 0

    params = load_mysql_base_params()
    with connect_ctx(params, database=args.master_db, autocommit=False) as conn:
        cur = conn.cursor()
        try:
            if args.replace:
                cur.execute(f"DELETE FROM {qname(args.master_db)}.`postal_code_addresses`")
            summary.inserted_or_updated = upsert_rows(cur, args.master_db, rows, args.batch_size)
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
