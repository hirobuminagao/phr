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
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

# VS Code の Run で直接実行した場合でも `scripts.*` を import できるようにする
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.csv.csv_loader import load_csv
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.lookup.fund import get_fund_id_from_insurer_number
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.identity.field.date_field import normalize_date_to_ymd_and_compact
from scripts.lib.identity.field.name_kana import (
    normalize_name_kana_full,
    normalize_name_kana_full_to_parts,
)
from scripts.lib.identity.field.name_kanji import (
    normalize_name_kanji_full,
    normalize_name_kanji_full_to_parts,
)
from scripts.lib.identity.base_norm import base_normalize


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
    "name_kanji_full_norm",
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


@dataclass
class RowErrorRecord:
    src_file: str
    src_row_no: int
    csv_header: str
    target_column: str
    rule: str
    raw_value: Any
    reason: str


@dataclass
class ProcessFileResult:
    inserted_row_count: int
    skipped_empty_row_count: int
    row_error_count: int
    row_errors: list[RowErrorRecord]


def validate_mapping_headers(loader: Any, mappings: list[MappingRow]) -> None:
    header_map = loader.get_header_dict()
    missing = sorted({m.csv_header for m in mappings if m.csv_header not in header_map})
    if missing:
        headers = list(header_map.keys())
        raise ValueError(
            "template_mappings.csv_header が CSV ヘッダーに存在しません: "
            f"missing={missing} available={headers}"
        )


def is_effectively_empty_row(source_row: Mapping[str, Any]) -> bool:
    """Excelエクスポート由来の全列空行をスキップ対象とする。"""
    key_fields = [
        source_row.get("記号"),
        source_row.get("番号"),
        source_row.get("氏名（カナ）"),
        source_row.get("氏名（漢字）"),
    ]
    return all((v is None or str(v).strip() == "") for v in key_fields)


def row_get_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if value is None:
        raise ValueError(f"missing column: {key}")
    return str(value)


def row_get_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise ValueError(f"missing column: {key}")
    return int(value)





def normalize_digits_or_none(value: str) -> str | None:
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def normalize_symbol(value: str) -> str | None:
    base = base_normalize(value)
    if base is None:
        return None

    text = base.replace(" ", "　")
    text = text.replace("－", "-").replace("ー", "-").replace("―", "-")
    return text or None




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
        result = normalize_date_to_ymd_and_compact(v, purpose="birthdate")
        if not result["ok"]:
            raise ValueError(f"birth_norm: {result['reason']}")
        return result["field_norm"]

    if rule == "gender_code_norm":
        return normalize_gender_code(v)

    if rule == "date_or_null":
        result = normalize_date_to_ymd_and_compact(v, purpose="date_field")
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "kana_full_no_space":
        result = normalize_name_kana_full(v)
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "name_kanji_full_norm":
        result = normalize_name_kanji_full(v)
        if not result["ok"]:
            return None
        return result["field_norm"]

    if rule == "split_family":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["family"]

    if rule == "split_middle":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["middle"]

    if rule == "split_given":
        result = normalize_name_kanji_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["given"]

    if rule == "split_family_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["family"]

    if rule == "split_middle_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["middle"]

    if rule == "split_given_kana":
        result = normalize_name_kana_full_to_parts(v)
        if not result["ok"]:
            return None
        return result["given"]

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
        row = cast(Mapping[str, Any] | None, cursor.fetchone())
    finally:
        cursor.close()

    if not row:
        raise ValueError(f"template not found: fund_id={fund_id}")

    return TemplateRow(
        fund_id=row_get_int(row, "fund_id"),
        version=row_get_int(row, "version"),
        template_type=row_get_str(row, "template_type"),
        target_table=row_get_str(row, "target_table"),
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
        rows = cast(list[Mapping[str, Any]], cursor.fetchall())
    finally:
        cursor.close()

    return [
        MappingRow(
            csv_header=row_get_str(row, "csv_header"),
            target_column=row_get_str(row, "target_column"),
            rule=row_get_str(row, "rule"),
            required=row_get_int(row, "required"),
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
        raw_value = source_row.get(m.csv_header)
        try:
            value = apply_rule(m.rule, raw_value)
        except Exception as e:
            raise ValueError(
                "rule apply failed: "
                f"header={m.csv_header} target={m.target_column} rule={m.rule} "
                f"raw_value={raw_value!r} src_file={src_file} src_row_no={src_row_no}"
            ) from e

        if m.required == 1 and (value is None or value == ""):
            raise ValueError(
                "required missing: "
                f"header={m.csv_header} target={m.target_column} rule={m.rule} "
                f"raw_value={raw_value!r} src_file={src_file} src_row_no={src_row_no}"
            )

        if m.target_column == "phone_norm" and row.get("phone_norm"):
            continue

        row[m.target_column] = value

    return row


def to_row_error_record(error: Exception, mappings: list[MappingRow], source_row: Mapping[str, Any], src_file: str, src_row_no: int) -> RowErrorRecord:
    """行エラーをログ用の構造へ変換する。"""
    message = str(error)

    for m in mappings:
        raw_value = source_row.get(m.csv_header)
        marker = f"header={m.csv_header} target={m.target_column} rule={m.rule}"
        if marker in message:
            return RowErrorRecord(
                src_file=src_file,
                src_row_no=src_row_no,
                csv_header=m.csv_header,
                target_column=m.target_column,
                rule=m.rule,
                raw_value=raw_value,
                reason=message,
            )

    return RowErrorRecord(
        src_file=src_file,
        src_row_no=src_row_no,
        csv_header="",
        target_column="",
        rule="",
        raw_value=None,
        reason=message,
    )


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


def process_file(conn: Any, insurer_number: str, path: Path) -> ProcessFileResult:
    fund_id = get_fund_id_from_insurer_number(insurer_number)
    template = fetch_latest_template(conn, fund_id)
    mappings = fetch_template_mappings(conn, fund_id, template.version)

    unsupported = sorted({m.rule for m in mappings if m.rule not in SUPPORTED_RULES})
    if unsupported:
        raise ValueError(f"unsupported rules found: {unsupported}")

    loader = load_csv(path=str(path), header_count=1)
    validate_mapping_headers(loader, mappings)

    inserted_row_count = 0
    skipped_empty_row_count = 0
    row_error_count = 0
    row_errors: list[RowErrorRecord] = []

    for i, row_src in enumerate(loader.iter_dict_rows(), start=1):
        if is_effectively_empty_row(row_src):
            skipped_empty_row_count += 1
            continue

        try:
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
            inserted_row_count += 1
        except ValueError as e:
            row_error_count += 1
            row_errors.append(to_row_error_record(e, mappings, row_src, path.name, i))
            continue

    return ProcessFileResult(
        inserted_row_count=inserted_row_count,
        skipped_empty_row_count=skipped_empty_row_count,
        row_error_count=row_error_count,
        row_errors=row_errors,
    )


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
            total_inserted = 0
            total_skipped_empty = 0
            total_row_errors = 0
            completed_with_errors = False

            for insurer_number, path in files:
                print(f"processing: {insurer_number} {path}")
                result = process_file(conn, insurer_number, path)
                total_inserted += result.inserted_row_count
                total_skipped_empty += result.skipped_empty_row_count
                total_row_errors += result.row_error_count

                print(f"inserted rows: {result.inserted_row_count}")
                print(f"skipped empty rows: {result.skipped_empty_row_count}")
                print(f"row errors: {result.row_error_count}")

                if result.row_errors:
                    completed_with_errors = True
                    print("[WARN] row errors detected:")
                    for row_error in result.row_errors[:20]:
                        print(
                            "  - "
                            f"src_file={row_error.src_file} "
                            f"src_row_no={row_error.src_row_no} "
                            f"csv_header={row_error.csv_header!r} "
                            f"target_column={row_error.target_column!r} "
                            f"rule={row_error.rule!r} "
                            f"raw_value={row_error.raw_value!r} "
                            f"reason={row_error.reason}"
                        )
                    if len(result.row_errors) > 20:
                        print(f"  ... omitted {len(result.row_errors) - 20} more row errors")

            conn.commit()

            print(f"total inserted rows: {total_inserted}")
            print(f"total skipped empty rows: {total_skipped_empty}")
            print(f"total row errors: {total_row_errors}")

            if completed_with_errors:
                print("run status: completed_with_errors")
            else:
                print("run status: success")
        except Exception:
            conn.rollback()
            print("run status: failed")
            raise


if __name__ == "__main__":
    main()