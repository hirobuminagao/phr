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
    hits_by_column_id: dict[int, list[dict[str, Any]]] = {}
    try:
        cur.execute(
            f"""
            SELECT
              `hit`.`analysis_column_id`,
              `hit`.`rule_id`,
              `hit`.`score`,
              `hit`.`reason`,
              `rule`.`scope`,
              `rule`.`condition_type`,
              `rule`.`header_pattern`,
              `rule`.`target_kind`,
              `rule`.`target_namecode`,
              `rule`.`target_ledger_field`,
              `rule`.`mapping_strategy`
            FROM {qname(lab_db)}.`csv_mapping_rule_hits` AS `hit`
            INNER JOIN {qname(lab_db)}.`csv_mapping_rules` AS `rule`
              ON `rule`.`rule_id` = `hit`.`rule_id`
            WHERE `hit`.`analysis_column_id` IN (
              SELECT `analysis_column_id`
              FROM {qname(lab_db)}.`analysis_columns`
              WHERE `analysis_file_id` = %s
            )
            ORDER BY `hit`.`analysis_column_id`, `hit`.`score` DESC
            """,
            (analysis_file_id,),
        )
        for row in cur.fetchall():
            hit = dict(row)
            hits_by_column_id.setdefault(int(hit["analysis_column_id"]), []).append(hit)
    except Exception:
        hits_by_column_id = {}
    cur.close()
    for column in columns:
        column["rule_hits"] = hits_by_column_id.get(int(column["analysis_column_id"]), [])
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
        "ai_review": {
            "status": column.get("ai_review_status") or "NOT_REVIEWED",
            "note": column.get("ai_review_note"),
            "reviewed_by": column.get("ai_reviewed_by"),
            "reviewed_at": column.get("ai_reviewed_at"),
        },
        "current_decision": {
            "status": column.get("decision_status"),
            "note": column.get("decision_note"),
            "seed_target": bool(column.get("seed_target")),
            "seed_exported": bool(column.get("seed_exported")),
        },
        "analysis_note": column.get("analysis_note"),
        "rule_hits": [
            {
                "rule_id": hit.get("rule_id"),
                "score": hit.get("score"),
                "scope": hit.get("scope"),
                "condition_type": hit.get("condition_type"),
                "header_pattern": hit.get("header_pattern"),
                "target_kind": hit.get("target_kind"),
                "target_namecode": hit.get("target_namecode"),
                "target_ledger_field": hit.get("target_ledger_field"),
                "mapping_strategy": hit.get("mapping_strategy"),
                "reason": hit.get("reason"),
            }
            for hit in column.get("rule_hits", [])
        ],
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
        "task": "Review CSV mapping candidates for health exam result import. Return JSON only, following response_schema. AI review updates are drafts; human review is required before seed generation.",
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
            "ai_review_is_not_final_decision": True,
            "final_decision_by_human": True,
            "do_not_emit_real_seed": True,
            "do_not_set_current_decision_status": True,
            "target_kinds": ["LEDGER_FIELD", "EXAM_ITEM_VALUE", "IGNORE", "REVIEW", "WATCH"],
            "mapping_strategies": ["DIRECT", "MULTI_COLUMN_JOIN", "DERIVED_CODE", "METHOD_SELECTION", "IGNORE", "NEEDS_CONFIRMATION", "WATCH_IF_PRESENT"],
            "ai_review_statuses": ["REVIEWED", "SKIPPED", "FAILED"],
            "notes": [
                "Prefer exact meaning over superficial header similarity.",
                "Use rule_hits as prior knowledge, but do not treat it as final when header/value evidence conflicts.",
                "Use machine_candidate as the current automatic guess. Keep it when correct, replace it when a better target is clear, or mark SKIPPED when judgment is not possible.",
                "Do not change current_decision.status. Human operators will later choose ADOPT/IGNORE/REVIEW/WATCH on the admin screen.",
                "Do not mark a column as IGNORE only because sample_values is empty or non_blank_count is 0. If the header indicates a meaningful exam item or ledger field but values are absent in this sample, use REVIEW with NEEDS_CONFIRMATION.",
                "Use WATCH with WATCH_IF_PRESENT when the column is not mapped now, but should be noticed later if future CSV files contain non-empty values.",
                "Use IGNORE only when the header meaning itself is outside the import scope, duplicated noise, or clearly not useful for mapping.",
                "Do not map facility judgement columns as exam values unless the target meaning is clear.",
                "For fasting/random triglyceride or glucose, mark NEEDS_CONFIRMATION when no discriminator exists.",
                "For repeated findings, suggest related columns and whether to join or keep separate.",
            ],
        },
        "columns": compact_columns,
        "response_schema": {
            "analysis_file_id": "number",
            "reviewed_by": "string, for example codex",
            "updates": [
                {
                    "column_no": "number",
                    "ai_review_status": "REVIEWED | SKIPPED | FAILED",
                    "candidate_target_kind": "LEDGER_FIELD | EXAM_ITEM_VALUE | IGNORE | REVIEW | WATCH | null",
                    "candidate_namecode": "string|null",
                    "candidate_ledger_field": "string|null",
                    "confidence": "0.0-1.0",
                    "mapping_strategy": "DIRECT | MULTI_COLUMN_JOIN | DERIVED_CODE | METHOD_SELECTION | IGNORE | NEEDS_CONFIRMATION | WATCH_IF_PRESENT",
                    "related_column_nos": ["number"],
                    "ai_review_note": "short Japanese explanation for DB ai_review_note",
                    "reason": "short Japanese explanation, can match ai_review_note",
                    "needs_human_review": "boolean",
                    "review_points": ["string"],
                    "candidate_action": "KEEP_MACHINE | REPLACE_MACHINE | MARK_IGNORE | MARK_REVIEW | MARK_WATCH | NO_CANDIDATE",
                }
            ],
            "notes": [
                "Use updates, not suggestions.",
                "REVIEWED means AI checked the row and gives a usable candidate or explicit IGNORE/REVIEW candidate.",
                "SKIPPED means AI intentionally leaves this row for human work because evidence is insufficient.",
                "FAILED means AI could not process the row due to malformed data or another error.",
                "WATCH means not imported now, but future non-empty values should be flagged for human review.",
                "candidate_namecode is required only when candidate_target_kind is EXAM_ITEM_VALUE.",
                "candidate_ledger_field is required only when candidate_target_kind is LEDGER_FIELD.",
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
