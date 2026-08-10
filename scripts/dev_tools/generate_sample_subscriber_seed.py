#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.lib.identity.field.insurance_number import normalize_insurance_number  # noqa: E402
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol  # noqa: E402
from scripts.lib.identity.field.name_kana import normalize_name_kana_full  # noqa: E402
from scripts.lib.identity.generator import generate_identity_bundle  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = (
    PROJECT_ROOT
    / "docs"
    / "spec"
    / "hia_fund_ledger_xml"
    / "samples"
    / "subscribers"
    / "sample_subscribers_event2.csv"
)


INSERT_COLUMNS = [
    "insurer_number",
    "insurance_symbol",
    "insurance_symbol_export",
    "insurance_number",
    "insurance_branchnumber",
    "birth",
    "gender_code",
    "name_kana_full",
    "name_kanji_full",
    "name_kana_full_match",
    "insurance_symbol_match",
    "insurance_number_match",
    "person_id_custom",
    "hia_subscriber_id",
    "identity_hash",
    "relationship_name",
    "qualification_acquired_date",
    "qualification_lost_date",
    "employee_code",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a local subscriber seed SQL from non-sensitive sample CSV.",
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--database", default="dev_phr")
    return parser.parse_args()


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if text == "":
        return "NULL"
    escaped = text.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def build_insert_row(row: dict[str, str]) -> dict[str, object]:
    symbol = normalize_insurance_symbol(row.get("insurance_symbol"))
    number = normalize_insurance_number(row.get("insurance_number"))
    kana = normalize_name_kana_full(row.get("name_kana_full"))
    identity = generate_identity_bundle(
        birthdate=row.get("birth"),
        insurer_number_raw=row.get("insurer_number"),
        insurance_symbol_raw=row.get("insurance_symbol"),
        insurance_number_raw=row.get("insurance_number"),
        name_kana_full_raw=row.get("name_kana_full"),
        gender_code=row.get("gender_code"),
    )

    if not identity.get("ok"):
        raise ValueError(f"{row.get('sample_key')}: identity build failed: {identity.get('reason')}")

    return {
        "insurer_number": row.get("insurer_number"),
        "insurance_symbol": row.get("insurance_symbol"),
        "insurance_symbol_export": symbol.get("export"),
        "insurance_number": row.get("insurance_number"),
        "insurance_branchnumber": row.get("insurance_branchnumber") or None,
        "birth": row.get("birth"),
        "gender_code": row.get("gender_code"),
        "name_kana_full": kana.get("field_norm") or row.get("name_kana_full"),
        "name_kanji_full": row.get("name_kanji_full"),
        "name_kana_full_match": kana.get("match"),
        "insurance_symbol_match": symbol.get("match"),
        "insurance_number_match": number.get("match"),
        "person_id_custom": identity.get("person_id_custom"),
        "hia_subscriber_id": row.get("hia_subscriber_id"),
        "identity_hash": identity.get("identity_hash"),
        "relationship_name": row.get("relationship_name"),
        "qualification_acquired_date": row.get("qualification_acquired_date") or None,
        "qualification_lost_date": row.get("qualification_lost_date") or None,
        "employee_code": row.get("employee_code") or None,
    }


def main() -> int:
    args = parse_args()
    with args.source.open("r", encoding="utf-8-sig", newline="") as fp:
        rows = [build_insert_row(row) for row in csv.DictReader(fp)]

    table = f"`{args.database}`.`subscribers`"
    print("-- Generated from docs/spec/hia_fund_ledger_xml/samples/subscribers/sample_subscribers_event2.csv")
    print("-- Local sample data only. Do not apply to production with real subscribers.")
    print(f"INSERT INTO {table} (")
    print("  " + ",\n  ".join(f"`{col}`" for col in INSERT_COLUMNS))
    print(") VALUES")
    value_lines = []
    for row in rows:
        values = ", ".join(sql_literal(row[col]) for col in INSERT_COLUMNS)
        value_lines.append(f"  ({values})")
    print(",\n".join(value_lines))
    print("ON DUPLICATE KEY UPDATE")
    update_columns = [col for col in INSERT_COLUMNS if col not in {"insurer_number", "insurance_symbol", "insurance_number"}]
    print("  " + ",\n  ".join(f"`{col}` = VALUES(`{col}`)" for col in update_columns) + ",")
    print("  `updated_at` = CURRENT_TIMESTAMP(3);")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
