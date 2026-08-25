#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export csv_mapping_lab analysis rows as an LLM prompt JSON."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import CSV_MAPPING_LAB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export CSV mapping analysis as LLM prompt JSON.")
    parser.add_argument("analysis_file_id", type=int)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--lab-db", default=CSV_MAPPING_LAB)
    parser.add_argument("--output", default=None, help="Output JSON path. Defaults to stdout.")
    parser.add_argument("--column-start", type=int, default=None)
    parser.add_argument("--column-end", type=int, default=None)
    parser.add_argument("--only-unreviewed", action="store_true")
    parser.add_argument("--include-sensitive", action="store_true")
    parser.add_argument("--nearby-window", type=int, default=3)
    return parser.parse_args()


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def decode_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def fetch_analysis(conn: Any, *, lab_db: str, analysis_file_id: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cur = dict_cursor(conn)
    cur.execute(
        f"""
        SELECT *
        FROM {qname(lab_db)}.`analysis_files`
        WHERE `analysis_file_id` = %s
        """,
        (analysis_file_id,),
    )
    file_row = cur.fetchone()
    if not file_row:
        raise RuntimeError(f"analysis_file_id not found: {analysis_file_id}")

    cur.execute(
        f"""
        SELECT *
        FROM {qname(lab_db)}.`analysis_columns`
        WHERE `analysis_file_id` = %s
        ORDER BY `column_no`
        """,
        (analysis_file_id,),
    )
    columns = [dict(row) for row in cur.fetchall()]
    cur.close()
    return dict(file_row), columns


def filter_columns(columns: list[dict[str, Any]], *, args: argparse.Namespace) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for column in columns:
        column_no = int(column["column_no"])
        if args.column_start is not None and column_no < args.column_start:
            continue
        if args.column_end is not None and column_no > args.column_end:
            continue
        if args.only_unreviewed and column.get("decision_status") != "UNREVIEWED":
            continue
        if not args.include_sensitive and int(column.get("sensitive_hint") or 0) == 1:
            continue
        result.append(column)
    return result


def compact_column(column: dict[str, Any], *, all_columns_by_no: dict[int, dict[str, Any]], nearby_window: int) -> dict[str, Any]:
    column_no = int(column["column_no"])
    nearby_headers: list[dict[str, Any]] = []
    for nearby_no in range(column_no - nearby_window, column_no + nearby_window + 1):
        if nearby_no == column_no:
            continue
        nearby = all_columns_by_no.get(nearby_no)
        if not nearby:
            continue
        nearby_headers.append(
            {
                "column_no": nearby_no,
                "header_name": nearby.get("header_name"),
                "inferred_value_type": nearby.get("inferred_value_type"),
            }
        )

    return {
        "column_no": column_no,
        "header_name": column.get("header_name"),
        "header_occurrence": column.get("header_occurrence"),
        "normalized_header_name": column.get("normalized_header_name"),
        "sample_values": decode_json_value(column.get("sample_values_json")) or [],
        "sample_value_counts": decode_json_value(column.get("sample_value_counts_json")) or [],
        "blank_count": column.get("blank_count"),
        "non_blank_count": column.get("non_blank_count"),
        "blank_rate": column.get("blank_rate"),
        "distinct_value_count": column.get("distinct_value_count"),
        "inferred_value_type": column.get("inferred_value_type"),
        "inferred_format": column.get("inferred_format"),
        "min_numeric_value": column.get("min_numeric_value"),
        "max_numeric_value": column.get("max_numeric_value"),
        "min_text_length": column.get("min_text_length"),
        "max_text_length": column.get("max_text_length"),
        "sensitive_hint": bool(column.get("sensitive_hint")),
        "nearby_headers": nearby_headers,
        "machine_candidate": {
            "target_kind": column.get("candidate_target_kind"),
            "namecode": column.get("candidate_namecode"),
            "ledger_field": column.get("candidate_ledger_field"),
            "confidence": column.get("candidate_confidence"),
        },
        "current_decision": {
            "status": column.get("decision_status"),
            "note": column.get("decision_note"),
            "seed_target": bool(column.get("seed_target")),
            "seed_exported": bool(column.get("seed_exported")),
        },
        "analysis_note": column.get("analysis_note"),
        "related_column_nos": decode_json_value(column.get("related_column_nos_json")) or [],
        "value_profile": decode_json_value(column.get("value_profile_json")) or {},
    }


def build_prompt(file_row: dict[str, Any], columns: list[dict[str, Any]], selected_columns: list[dict[str, Any]], *, args: argparse.Namespace) -> dict[str, Any]:
    all_columns_by_no = {int(column["column_no"]): column for column in columns}
    compact_columns = [
        compact_column(column, all_columns_by_no=all_columns_by_no, nearby_window=args.nearby_window)
        for column in selected_columns
    ]
    return {
        "task": "Suggest CSV mapping candidates for health exam result import. Return JSON only, following response_schema. Suggestions are drafts; human review is required before seed generation.",
        "analysis_file": {
            "analysis_file_id": file_row.get("analysis_file_id"),
            "source_file_name": file_row.get("source_file_name"),
            "facility_code": file_row.get("facility_code"),
            "facility_name": file_row.get("facility_name"),
            "encoding": file_row.get("encoding"),
            "delimiter": file_row.get("delimiter"),
            "header_row_no": file_row.get("header_row_no"),
            "data_start_row_no": file_row.get("data_start_row_no"),
            "row_count": file_row.get("row_count"),
            "column_count": file_row.get("column_count"),
            "header_sha256": file_row.get("header_sha256"),
            "analysis_status": file_row.get("analysis_status"),
            "memo": file_row.get("memo"),
        },
        "selection": {
            "column_start": args.column_start,
            "column_end": args.column_end,
            "only_unreviewed": args.only_unreviewed,
            "include_sensitive": args.include_sensitive,
            "nearby_window": args.nearby_window,
            "selected_column_count": len(compact_columns),
            "total_column_count": len(columns),
        },
        "mapping_policy": {
            "final_decision_by_human": True,
            "do_not_emit_real_seed": True,
            "target_kinds": ["LEDGER_FIELD", "EXAM_ITEM_VALUE", "IGNORE", "REVIEW"],
            "mapping_strategies": ["DIRECT", "MULTI_COLUMN_JOIN", "DERIVED_CODE", "METHOD_SELECTION", "IGNORE", "NEEDS_CONFIRMATION"],
            "notes": [
                "Prefer exact meaning over superficial header similarity.",
                "Do not map facility judgement columns as exam values unless the target meaning is clear.",
                "For fasting/random triglyceride or glucose, mark NEEDS_CONFIRMATION when no discriminator exists.",
                "For repeated findings, suggest related columns and whether to join or keep separate.",
            ],
        },
        "columns": compact_columns,
        "response_schema": {
            "analysis_file_id": "number",
            "suggestions": [
                {
                    "column_no": "number",
                    "target_kind": "LEDGER_FIELD | EXAM_ITEM_VALUE | IGNORE | REVIEW",
                    "candidate_namecode": "string|null",
                    "candidate_ledger_field": "string|null",
                    "confidence": "0.0-1.0",
                    "mapping_strategy": "DIRECT | MULTI_COLUMN_JOIN | DERIVED_CODE | METHOD_SELECTION | IGNORE | NEEDS_CONFIRMATION",
                    "related_column_nos": ["number"],
                    "reason": "short Japanese explanation",
                    "needs_human_review": "boolean",
                    "review_points": ["string"],
                }
            ],
        },
    }


def main() -> int:
    args = parse_args()
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=args.lab_db, autocommit=True) as conn:
        file_row, columns = fetch_analysis(conn, lab_db=args.lab_db, analysis_file_id=args.analysis_file_id)

    selected_columns = filter_columns(columns, args=args)
    prompt = build_prompt(file_row, columns, selected_columns, args=args)
    payload = json.dumps(prompt, ensure_ascii=False, indent=2, default=json_default)

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
        print(str(output_path))
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
