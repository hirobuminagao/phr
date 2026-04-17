

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
import_staging_subscribers_fund.py

健保受領CSVを staging_subscribers_fund へ取り込む最小実装版。

今回版の目的:
- input/<insurer_number>/ 配下のCSVを取得する
- csv_loader でCSVを読む
- insurer_number -> fund_id を lookup で解決する
- templates / template_mappings を取得する
- mapping に従って *_norm / 補助列を生成する
- insurer_number_norm をスクリプト側で注入する
- staging_subscribers_fund へ INSERT する

今回版ではまだ行わないもの:
- *_match 生成
- person_id_custom / identity_hash 生成
- matched_subscriber_id 生成
- archive 移動
- 詳細なエラー蓄積
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.lib.csv.csv_loader import load_csv
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.lookup.fund import get_fund_id_from_insurer_number
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR


DEFAULT_INPUT_BASE_DIR = Path("data/from_fund/import_subscribers_staging/input")
SUPPORTED_RULES = {
    "as_is",
    "symbol_norm",
    "symbol_digits",
    "digits_required",
    "digits_or_null",
    "birth_norm",
    "gender_code_norm",
    "date_or_null",
    "kana_full_no_space",
    "split_family",
    "split_middle",
    "split_given",
    "split_family_kana",
    "split_middle_kana",
    "split_given_kana",
}


@dataclass
class TemplateRow:
    fund_id: int
    version: int
    template_type: str
    target_table: str


@dataclass
class MappingRow:
    csv_header: str
    target_column: str
    rule: str
    required: int


def normalize_spaces_to_fullwidth(value: str) -> str:
    text = value.replace("\u3000", " ")
    parts = [p for p in text.strip().split() if p != ""]
    return "　".join(parts)


def normalize_kana_full_no_space(value: str) -> str | None:
    text = normalize_spaces_to_fullwidth(value)
    result = text.replace("　", "")
    return result or None


def split_name_parts(value: str) -> tuple[str | None, str | None, str | None]:
    text = normalize_spaces_to_fullwidth(value)
    if text == "":
        return None, None, None

    parts = [p for p in text.split("　") if p != ""]
    if len(parts) == 0:
        return None, None, None
    if len(parts) == 1:
        return parts[0], None, None
    if len(parts) == 2:
        return parts[0], None, parts[1]
    return parts[0], "　".join(parts[1:-1]), parts[-1]


def normalize_digits_or_none(value: str) -> str | None:
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def normalize_symbol(value: str) -> str | None:
    text = normalize_spaces_to_fullwidth(value)
    text = text.replace("－", "-").replace("ー", "-").replace("―", "-")
    return text or None


def normalize_birth(value: str) -> str | None:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
    return None


def normalize_date_or_null(value: str) -> str | None:
    return normalize_birth(value)


def normalize_gender_code(value: str) -> int | None:
    text = str(value).strip()
    if text in {"1", "男", "男性"}:
        return 1
    if text in {"2", "女", "女性"}:
        return 2
    if text in {"9", "0", ""}:
        return None
    return None


def apply_rule(rule: str, value: str | None) -> Any:
    if value is None:
        return None
    v = str(value).strip()

    if rule == "as_is":
        return v or None

    if rule == "symbol_norm":
        return normalize_symbol(v)

    if rule == "symbol_digits":
        digits = normalize_digits_or_none(v)
        return int(digits) if digits else None

    if rule == "digits_required":
        digits = normalize_digits_or_none(v)
        if not digits:
            raise ValueError("digits_required: digits not found")
        return digits

    if rule == "digits_or_null":
        return normalize_digits_or_none(v)

    if rule == "birth_norm":
        normalized = normalize_birth(v)
        if not normalized:
            raise ValueError("birth_norm: invalid birth value")
        return normalized

    if rule == "gender_code_norm":
        return normalize_gender_code(v)

    if rule == "date_or_null":
        return normalize_date_or_null(v)

    if rule == "kana_full_no_space":
        return normalize_kana_full_no_space(v)

    if rule == "split_family":
        family, _, _ = split_name_parts(v)
        return family

    if rule == "split_middle":
        _, middle, _ = split_name_parts(v)
        return middle

    if rule == "split_given":
        _, _, given = split_name_parts(v)
        return given

    if rule == "split_family_kana":
        family, _, _ = split_name_parts(v)
        return family

    if rule == "split_middle_kana":
        _, middle, _ = split_name_parts(v)
        return middle

    if rule == "split_given_kana":
        _, _, given = split_name_parts(v)
        return given

    raise ValueError(f"unsupported rule: {rule}")


def fetch_latest_template(conn: Any, fund_id: int) -> TemplateRow:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT fund_id, version, template_type, target_table
            FROM {DEV_PHR}.templates
            WHERE fund_id = %s
            ORDER BY version DESC
            LIMIT 1
            """,
            (fund_id,),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()

    if not row:
        raise ValueError(f"template not found: fund_id={fund_id}")

    return TemplateRow(
        fund_id=int(row["fund_id"]),
        version=int(row["version"]),
        template_type=row["template_type"],
        target_table=row["target_table"],
    )


def fetch_template_mappings(conn: Any, fund_id: int, version: int) -> list[MappingRow]:
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT csv_header, target_column, rule, required
            FROM {DEV_PHR}.template_mappings
            WHERE fund_id = %s AND version = %s
            ORDER BY col_order, target_column
            """,
            (fund_id, version),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [
        MappingRow(
            csv_header=row["csv_header"],
            target_column=row["target_column"],
            rule=row["rule"],
            required=int(row["required"]),
        )
        for row in rows
    ]


def build_row(
    fund_id: int,
    version: int,
    insurer_number: str,
    src_file: str,
    src_row_no: int,
    source_row: dict[str, Any],
    mappings: list[MappingRow],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "fund_id": fund_id,
        "version": version,
        "insurer_number_norm": insurer_number.zfill(8),
        "src_file": src_file,
        "src_row_no": src_row_no,
        "src_line_no": src_row_no + 1,
    }

    for m in mappings:
        value = apply_rule(m.rule, source_row.get(m.csv_header))

        if m.required == 1 and (value is None or value == ""):
            raise ValueError(f"required missing: {m.csv_header}")

        if m.target_column == "phone_norm" and row.get("phone_norm"):
            continue

        row[m.target_column] = value

    return row


def insert_row(conn: Any, row: dict[str, Any]) -> None:
    cols = sorted(row.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(f"`{c}`" for c in cols)
    sql = f"""
        INSERT INTO {DEV_PHR}.staging_subscribers_fund
        ({col_sql})
        VALUES ({placeholders})
    """
    values = [row[c] for c in cols]

    cursor = dict_cursor(conn)
    try:
        cursor.execute(sql, tuple(values))
    finally:
        cursor.close()


def process_file(conn: Any, insurer_number: str, path: Path) -> int:
    fund_id = get_fund_id_from_insurer_number(insurer_number)
    template = fetch_latest_template(conn, fund_id)
    mappings = fetch_template_mappings(conn, fund_id, template.version)

    unsupported = sorted({m.rule for m in mappings if m.rule not in SUPPORTED_RULES})
    if unsupported:
        raise ValueError(f"unsupported rules found: {unsupported}")

    loader = load_csv(path=str(path), header_count=1)

    count = 0
    for i, row_src in enumerate(loader.iter_dict_rows(), start=1):
        row = build_row(
            fund_id,
            template.version,
            insurer_number,
            path.name,
            i,
            row_src,
            mappings,
        )
        insert_row(conn, row)
        count += 1

    return count


def list_files(base: Path) -> list[tuple[str, Path]]:
    results: list[tuple[str, Path]] = []
    if not base.exists():
        return results

    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.csv")):
            results.append((d.name, f))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-base-dir", default=str(DEFAULT_INPUT_BASE_DIR))
    args = parser.parse_args()

    base = Path(args.input_base_dir)
    files = list_files(base)

    if not files:
        print(f"[INFO] no csv files found under: {base}")
        return

    params = load_mysql_base_params()

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        try:
            total = 0
            for insurer_number, path in files:
                print(f"processing: {insurer_number} {path}")
                inserted = process_file(conn, insurer_number, path)
                total += inserted
                print(f"inserted rows: {inserted}")
            conn.commit()
            print(f"total inserted rows: {total}")
        except Exception:
            conn.rollback()
            raise


if __name__ == "__main__":
    main()