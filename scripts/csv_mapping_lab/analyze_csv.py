#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Analyze a health exam CSV into the csv_mapping_lab schema."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.csv.csv_loader import CsvHeaderSet, load_csv_result
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import CSV_MAPPING_LAB


SENSITIVE_HEADER_KEYWORDS = (
    "氏名",
    "カナ",
    "かな",
    "生年月日",
    "性別",
    "保険証",
    "記号",
    "番号",
    "枝番",
    "社員番号",
    "加入者",
    "受診券",
    "住所",
    "電話",
    "郵便",
)


LEDGER_FIELD_HINTS = {
    "社員番号": "person_id_custom",
    "保険証記号": "insurance_symbol_raw",
    "保険記号": "insurance_symbol_raw",
    "記号": "insurance_symbol_raw",
    "保険証番号": "insurance_number_raw",
    "保険番号": "insurance_number_raw",
    "番号": "insurance_number_raw",
    "枝番": "insurance_branch_number_raw",
    "カナ氏名": "name_kana_raw",
    "氏名カナ": "name_kana_raw",
    "漢字氏名": "name_full_raw",
    "氏名": "name_full_raw",
    "性別": "gender_raw",
    "生年月日": "birthdate",
    "受診日": "exam_date",
    "受診日（西暦）": "exam_date",
    "健診機関番号": "facility_code",
    "健診機関コード": "facility_code",
    "健診機関名称": "facility_name",
    "健診機関名": "facility_name",
    "保険者番号": "insurer_number",
    "郵便番号": "postal_code",
    "住所": "address",
}


NAMECODE_HINTS = {
    "身長": "9N001000000000001",
    "体重": "9N006000000000001",
    "BMI": "9N011000000000001",
    "ＢＭＩ": "9N011000000000001",
    "肥満度": "9N026000000000002",
    "腹囲": "9N016160100000001",
    "尿蛋白": "1A010000000190111",
    "尿糖": "1A020000000190111",
    "糖": "1A020000000190111",
    "赤血球": "2A020000001930101",
    "血色素": "2A030000001930101",
    "ヘマトクリット": "2A040000001930102",
    "ﾍﾏﾄｸﾘｯﾄ": "2A040000001930102",
    "GOT": "3B035000002327201",
    "AST": "3B035000002327201",
    "GPT": "3B045000002327201",
    "ALT": "3B045000002327201",
    "γ-GTP": "3B090000002327101",
    "中性脂肪": "3F015000002327101",
    "HDLコレステロール": "3F070000002327101",
    "HDLｺﾚｽﾃﾛｰﾙ": "3F070000002327101",
    "LDLコレステロール": "3F077000002327101",
    "LDLCHO": "3F077000002327101",
    "non-HDLコレステロール": "3F069000002391901",
    "non-HDLｺﾚｽﾃﾛｰﾙ": "3F069000002391901",
    "血糖": "3D010000001927201",
    "HbA1c": "3D046000001920402",
    "クレアチニン": "3C015000002327101",
    "eGFR": "8A065000002391901",
    "安静心電図": "9A110160800000049",
    "胸部Ｘ線": "9N206160800000049",
    "胸部X線": "9N206160800000049",
    "既往歴": "9N056160400000049",
    "自覚症状": "9N061160800000049",
    "他覚症状": "9N066160800000049",
    "メタボリック判定": "9N501000000000011",
    "保健指導レベル": "9N506000000000011",
    "咀嚼": "9N821000000000011",
}

NAMECODE_PREFIX_HINTS = {
    "安静心電図": "9A110160800000049",
    "胸部Ｘ線": "9N206160800000049",
    "胸部X線": "9N206160800000049",
    "他覚症状(": "9N066160800000049",
    "他覚症状（": "9N066160800000049",
}


@dataclass(frozen=True)
class ColumnProfile:
    sample_values: list[str]
    value_counts: list[dict[str, Any]]
    distinct_value_count: int
    blank_count: int
    non_blank_count: int
    blank_rate: Decimal
    min_numeric_value: Decimal | None
    max_numeric_value: Decimal | None
    min_text_length: int | None
    max_text_length: int | None
    first_non_blank_row_no: int | None
    last_non_blank_row_no: int | None
    inferred_value_type: str
    inferred_format: str | None
    value_profile: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze a CSV into csv_mapping_lab.")
    parser.add_argument("csv_path", help="CSV file path")
    parser.add_argument("--facility-code", default=None)
    parser.add_argument("--facility-name", default=None)
    parser.add_argument("--source-folder-name", default=None)
    parser.add_argument("--header-row-no", type=int, default=1)
    parser.add_argument("--data-start-row-no", type=int, default=2)
    parser.add_argument("--encoding", default=None)
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--quote-char", default='"')
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--lab-db", default=CSV_MAPPING_LAB)
    parser.add_argument("--created-by", default=None)
    parser.add_argument("--memo", default=None)
    parser.add_argument("--replace-source-sha", action="store_true", help="Delete earlier analysis rows with the same file SHA before insert")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_header(value: str | None) -> str | None:
    text = compact_text(value)
    if not text:
        return None
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", "", text)
    return text.upper()


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def header_snapshot(header_set: CsvHeaderSet) -> dict[str, Any]:
    return {
        "active_header_row_no": header_set.active_header_row_no,
        "header_rows": header_set.header_rows,
        "normalized_columns": [
            {
                "column_no": column.column_no,
                "context": column.context,
                "header_name": column.header_name,
                "occurrence": column.occurrence,
            }
            for column in header_set.normalized_columns
        ],
    }


def as_decimal(value: str) -> Decimal | None:
    text = value.replace(",", "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def looks_like_date(values: list[str]) -> str | None:
    patterns = [
        ("YYYY-MM-DD", ("%Y-%m-%d",)),
        ("YYYY/MM/DD", ("%Y/%m/%d",)),
        ("YYYYMMDD", ("%Y%m%d",)),
        ("YYYY-MM", ("%Y-%m",)),
        ("YYYY/MM", ("%Y/%m",)),
    ]
    for label, formats in patterns:
        ok = 0
        for value in values:
            for fmt in formats:
                try:
                    datetime.strptime(value, fmt)
                    ok += 1
                    break
                except ValueError:
                    continue
        if values and ok == len(values):
            return label
    return None


def infer_profile(rows: list[list[str]], column_index: int, *, sample_limit: int) -> ColumnProfile:
    values: list[tuple[int, str]] = []
    blank_count = 0
    for row_offset, row in enumerate(rows, start=2):
        raw = row[column_index] if column_index < len(row) else ""
        value = str(raw).strip()
        if not value:
            blank_count += 1
        else:
            values.append((row_offset, value))

    non_blank_values = [value for _, value in values]
    non_blank_count = len(non_blank_values)
    total = blank_count + non_blank_count
    blank_rate = Decimal(blank_count) / Decimal(total) if total else Decimal("0")
    counts = Counter(non_blank_values)
    sample_values = list(dict.fromkeys(non_blank_values))[:sample_limit]
    value_counts = [
        {"value": value, "count": count}
        for value, count in counts.most_common(sample_limit)
    ]

    numeric_values = [as_decimal(value) for value in non_blank_values]
    numeric_only = non_blank_count > 0 and all(value is not None for value in numeric_values)
    date_format = looks_like_date(non_blank_values[:sample_limit])

    if non_blank_count == 0:
        inferred_type = "EMPTY"
        inferred_format = None
    elif date_format:
        inferred_type = "DATE"
        inferred_format = date_format
    elif numeric_only:
        inferred_type = "NUMERIC"
        inferred_format = "integer" if all("." not in value for value in non_blank_values) else "decimal"
    elif len(counts) <= min(20, max(3, non_blank_count)):
        inferred_type = "CODE"
        inferred_format = None
    else:
        inferred_type = "TEXT"
        inferred_format = None

    non_null_numeric = [value for value in numeric_values if value is not None]
    text_lengths = [len(value) for value in non_blank_values]
    first_row = values[0][0] if values else None
    last_row = values[-1][0] if values else None

    return ColumnProfile(
        sample_values=sample_values,
        value_counts=value_counts,
        distinct_value_count=len(counts),
        blank_count=blank_count,
        non_blank_count=non_blank_count,
        blank_rate=blank_rate.quantize(Decimal("0.0001")),
        min_numeric_value=min(non_null_numeric) if numeric_only and non_null_numeric else None,
        max_numeric_value=max(non_null_numeric) if numeric_only and non_null_numeric else None,
        min_text_length=min(text_lengths) if text_lengths else None,
        max_text_length=max(text_lengths) if text_lengths else None,
        first_non_blank_row_no=first_row,
        last_non_blank_row_no=last_row,
        inferred_value_type=inferred_type,
        inferred_format=inferred_format,
        value_profile={
            "sample_limit": sample_limit,
            "numeric_only": numeric_only,
            "date_format": date_format,
            "distinct_limited": len(counts) <= sample_limit,
        },
    )


def is_sensitive_header(header_name: str | None) -> bool:
    text = compact_text(header_name)
    if not text:
        return False
    return any(keyword in text for keyword in SENSITIVE_HEADER_KEYWORDS)


def candidate_for_header(header_name: str | None) -> tuple[str | None, str | None, str | None, Decimal | None]:
    text = compact_text(header_name)
    if not text:
        return None, None, None, None

    normalized = normalize_header(text) or ""
    is_judgement_or_reason = any(token in text for token in ("判定", "疑い", "実施理由"))
    for key, field in LEDGER_FIELD_HINTS.items():
        if normalize_header(key) == normalized:
            return "LEDGER_FIELD", None, field, Decimal("0.9500")

    for key, namecode in NAMECODE_HINTS.items():
        norm_key = normalize_header(key) or ""
        if norm_key == normalized:
            return "EXAM_ITEM_VALUE", namecode, None, Decimal("0.9500")

    for key, namecode in NAMECODE_PREFIX_HINTS.items():
        norm_key = normalize_header(key) or ""
        if norm_key and normalized.startswith(norm_key) and not is_judgement_or_reason:
            return "EXAM_ITEM_VALUE", namecode, None, Decimal("0.7000")

    return None, None, None, None


def insert_analysis(conn: Any, *, args: argparse.Namespace, csv_result: Any, parse_status: str, parse_error: str | None) -> int:
    csv_path = Path(args.csv_path).expanduser().resolve()
    source_sha = sha256_file(csv_path) if csv_path.exists() else None
    stat = csv_path.stat() if csv_path.exists() else None
    snapshot = header_snapshot(csv_result.header_set)

    cur = dict_cursor(conn)
    if args.replace_source_sha and source_sha:
        cur.execute(
            f"""
            SELECT `analysis_file_id`
            FROM `{args.lab_db}`.`analysis_files`
            WHERE `source_file_sha256` = %s
            """,
            (source_sha,),
        )
        old_ids = [int(row["analysis_file_id"]) for row in cur.fetchall()]
        if old_ids:
            placeholders = ", ".join(["%s"] * len(old_ids))
            cur.execute(
                f"DELETE FROM `{args.lab_db}`.`analysis_columns` WHERE `analysis_file_id` IN ({placeholders})",
                tuple(old_ids),
            )
            cur.execute(
                f"DELETE FROM `{args.lab_db}`.`analysis_files` WHERE `analysis_file_id` IN ({placeholders})",
                tuple(old_ids),
            )

    cur.execute(
        f"""
        INSERT INTO `{args.lab_db}`.`analysis_files` (
          `source_file_name`, `source_file_path`, `source_file_size_bytes`, `source_file_sha256`,
          `source_folder_name`, `facility_code`, `facility_name`, `payment_fund_code`, `payment_fund_name`,
          `encoding`, `delimiter`, `quote_char`, `header_row_no`, `data_start_row_no`,
          `row_count`, `column_count`, `header_sha256`, `header_snapshot_json`, `sample_row_count`,
          `parse_status`, `parse_error_message`, `analysis_status`, `memo`, `created_by`, `updated_by`
        ) VALUES (
          %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, CAST(%s AS JSON), %s,
          %s, %s, %s, %s, %s, %s
        )
        """,
        (
            csv_path.name,
            str(csv_path),
            stat.st_size if stat else None,
            source_sha,
            args.source_folder_name,
            args.facility_code,
            args.facility_name,
            args.facility_code,
            args.facility_name,
            csv_result.encoding,
            args.delimiter,
            args.quote_char,
            args.header_row_no,
            args.data_start_row_no,
            len(csv_result.rows),
            len(csv_result.header_set.normalized_columns),
            csv_result.header_set.header_sha256,
            json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
            min(len(csv_result.rows), args.sample_limit),
            parse_status,
            parse_error,
            "ANALYZED" if parse_status in {"OK", "WARNING"} else "NEW",
            args.memo,
            args.created_by,
            args.created_by,
        ),
    )
    analysis_file_id = int(cur.lastrowid)

    for column in csv_result.header_set.normalized_columns:
        profile = infer_profile(csv_result.rows, column.column_no - 1, sample_limit=args.sample_limit)
        target_kind, namecode, ledger_field, confidence = candidate_for_header(column.header_name)
        cur.execute(
            f"""
            INSERT INTO `{args.lab_db}`.`analysis_columns` (
              `analysis_file_id`, `column_no`, `header_occurrence`, `header_name`, `normalized_header_name`,
              `sample_values_json`, `sample_value_counts_json`, `distinct_value_count`,
              `blank_count`, `non_blank_count`, `blank_rate`,
              `min_numeric_value`, `max_numeric_value`, `min_text_length`, `max_text_length`,
              `first_non_blank_row_no`, `last_non_blank_row_no`,
              `inferred_value_type`, `inferred_format`, `sensitive_hint`, `value_profile_json`,
              `candidate_target_kind`, `candidate_namecode`, `candidate_ledger_field`, `candidate_confidence`
            ) VALUES (
              %s, %s, %s, %s, %s,
              CAST(%s AS JSON), CAST(%s AS JSON), %s,
              %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s,
              %s, %s, %s, CAST(%s AS JSON),
              %s, %s, %s, %s
            )
            """,
            (
                analysis_file_id,
                column.column_no,
                column.occurrence,
                column.header_name,
                normalize_header(column.header_name),
                json.dumps(profile.sample_values, ensure_ascii=False, separators=(",", ":")),
                json.dumps(profile.value_counts, ensure_ascii=False, separators=(",", ":")),
                profile.distinct_value_count,
                profile.blank_count,
                profile.non_blank_count,
                profile.blank_rate,
                profile.min_numeric_value,
                profile.max_numeric_value,
                profile.min_text_length,
                profile.max_text_length,
                profile.first_non_blank_row_no,
                profile.last_non_blank_row_no,
                profile.inferred_value_type,
                profile.inferred_format,
                1 if is_sensitive_header(column.header_name) else 0,
                json.dumps(profile.value_profile, ensure_ascii=False, separators=(",", ":")),
                target_kind,
                namecode,
                ledger_field,
                confidence,
            ),
        )

    cur.close()
    return analysis_file_id


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 2

    try:
        csv_result = load_csv_result(
            str(csv_path),
            header_count=args.header_row_no,
            delimiter=args.delimiter,
            encoding=args.encoding,
            quote_char=args.quote_char,
            active_header_row_no=args.header_row_no,
            data_start_row_no=args.data_start_row_no,
        )
        parse_status = "OK"
        parse_error = None
        expected_columns = len(csv_result.header_set.normalized_columns)
        row_widths = {len(row) for row in csv_result.rows}
        if any(width != expected_columns for width in row_widths):
            parse_status = "WARNING"
            parse_error = f"row column count mismatch: header={expected_columns}, row_widths={sorted(row_widths)[:10]}"
    except Exception as exc:
        print(f"CSV parse failed: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(
            json.dumps(
                {
                    "source_file_name": csv_path.name,
                    "encoding": csv_result.encoding,
                    "rows": len(csv_result.rows),
                    "columns": len(csv_result.header_set.normalized_columns),
                    "header_sha256": csv_result.header_set.header_sha256,
                    "parse_status": parse_status,
                    "parse_error_message": parse_error,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.lab_db, autocommit=False) as conn:
        try:
            analysis_file_id = insert_analysis(
                conn,
                args=args,
                csv_result=csv_result,
                parse_status=parse_status,
                parse_error=parse_error,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(
        json.dumps(
            {
                "analysis_file_id": analysis_file_id,
                "source_file_name": csv_path.name,
                "encoding": csv_result.encoding,
                "rows": len(csv_result.rows),
                "columns": len(csv_result.header_set.normalized_columns),
                "header_sha256": csv_result.header_set.header_sha256,
                "parse_status": parse_status,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
