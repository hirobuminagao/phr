#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build adopted output values for person-level XML export cases."""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import start_run as etl_start_run
from scripts.from_medical.script_lib.export_case_readiness import refresh_export_case_readiness


HEALTH_DB = "health_exam_result"
ETL_PHASE = "BUILD_EXAM_EXPORT_CASE_VALUES"
ETL_SOURCE = "FROM_MEDICAL"


@dataclass(frozen=True)
class BuildValueConfig:
    event_id: int
    health_db: str
    master_db: str
    dry_run: bool
    limit_cases: int
    include_review_required: bool
    case_ids: tuple[int, ...] = ()


@dataclass
class BuildValueSummary:
    event_id: int
    dry_run: bool
    cases_seen: int = 0
    cases_built: int = 0
    cases_skipped: int = 0
    values_deleted: int = 0
    values_inserted: int = 0
    precedence_rules_applied: int = 0
    errors: int = 0

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            rows_seen=self.cases_seen,
            rows_inserted=self.values_inserted,
            rows_updated=self.cases_built,
            rows_skipped=self.cases_skipped,
            errors=self.errors,
        )

    def message(self) -> str:
        return (
            f"build_exam_export_case_values event_id={self.event_id} cases_seen={self.cases_seen} "
            f"cases_built={self.cases_built} cases_skipped={self.cases_skipped} "
            f"values_deleted={self.values_deleted} values_inserted={self.values_inserted} "
            f"precedence_rules_applied={self.precedence_rules_applied} errors={self.errors} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build adopted case values from source exam_item_values.")
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-cases", type=int, default=0)
    parser.add_argument("--case-id", type=int, action="append", default=[])
    parser.add_argument("--include-review-required", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    parser.add_argument("--master-db", default="phr_master")
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: BuildValueConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    if config.limit_cases < 0:
        raise ValueError("limit_cases must be >= 0")
    if any(case_id <= 0 for case_id in config.case_ids):
        raise ValueError("case_id must be positive")
    qname(config.health_db)
    qname(config.master_db)


def fetch_cases(cur: Any, config: BuildValueConfig) -> list[dict[str, Any]]:
    filters = ["`event_id` = %s", "`case_lifecycle_status` = 'ACTIVE'"]
    params: list[Any] = [config.event_id]
    if not config.include_review_required:
        filters.append("`merge_status` <> 'REVIEW_REQUIRED'")
    if config.case_ids:
        filters.append(f"`exam_export_case_id` IN ({', '.join(['%s'] * len(config.case_ids))})")
        params.extend(config.case_ids)
    limit_sql = ""
    if config.limit_cases:
        limit_sql = "LIMIT %s"
        params.append(config.limit_cases)
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.`exam_export_cases`
        WHERE {' AND '.join(filters)}
        ORDER BY `subscriber_id`, `exam_date`, `exam_facility_id`, `exam_export_case_id`
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_case_items(cur: Any, config: BuildValueConfig, case_row: dict[str, Any]) -> list[dict[str, Any]]:
    case_id = int(case_row["exam_export_case_id"])
    exam_facility_id = case_row.get("exam_facility_id")
    cur.execute(
        f"""
        SELECT
          src.`source_priority`,
          src.`source_role`,
          src.`source_exam_ledger_id`,
          src.`source_type`,
          eiv.`id` AS `source_exam_item_value_id`,
          eiv.`event_id`,
          eiv.`subscriber_id`,
          eiv.`hia_subscriber_id`,
          eiv.`namecode`,
          eiv.`occurrence_no`,
          eiv.`section_code`,
          eiv.`section_code_system`,
          eiv.`section_name`,
          eiv.`raw_value`,
          eiv.`normalized_value`,
          eiv.`normalized_unit`,
          eiv.`nullflavor`,
          eiv.`code_value`,
          eiv.`code_display`,
          eiv.`interpretation_code`,
          eiv.`interpretation_name`,
          eiv.`source_reference_lower`,
          eiv.`source_reference_upper`,
          eiv.`negation_ind`,
          COALESCE(fpolicy.`output_policy`, gpolicy.`output_policy`, 'INCLUDE') AS `output_policy`,
          COALESCE(fpolicy.`policy_reason`, gpolicy.`policy_reason`) AS `output_policy_reason`
        FROM {qname(config.health_db)}.`exam_export_case_sources` AS src
        INNER JOIN {qname(config.health_db)}.`exam_item_values` AS eiv
          ON eiv.`ledger_type` = 'EXAM'
         AND eiv.`ledger_id` = src.`source_exam_ledger_id`
        LEFT JOIN {qname(config.master_db)}.`exam_item_output_policies` AS fpolicy
          ON fpolicy.`exam_facility_id` = %s
         AND fpolicy.`namecode` = eiv.`namecode`
         AND fpolicy.`is_active` = 1
        LEFT JOIN {qname(config.master_db)}.`exam_item_output_policies` AS gpolicy
          ON gpolicy.`exam_facility_id` = 0
         AND gpolicy.`namecode` = eiv.`namecode`
         AND gpolicy.`is_active` = 1
        WHERE src.`exam_export_case_id` = %s
          AND src.`source_status` = 'ACTIVE'
          AND eiv.`validation_status` = 'VALID'
          AND eiv.`namecode` IS NOT NULL
          AND COALESCE(fpolicy.`output_policy`, gpolicy.`output_policy`, 'INCLUDE') <> 'EXCLUDE'
        ORDER BY src.`source_priority`, eiv.`namecode`, eiv.`occurrence_no`, eiv.`id`
        """,
        (exam_facility_id, case_id),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_precedence_rules(cur: Any, config: BuildValueConfig, case_row: dict[str, Any]) -> dict[tuple[str, int], list[dict[str, Any]]]:
    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.`exam_item_value_precedence_rules`
        WHERE (`event_id` IS NULL OR `event_id` = %s)
          AND (`exam_facility_id` IS NULL OR `exam_facility_id` = %s)
          AND `is_active` = 1
        ORDER BY
          CASE WHEN `event_id` IS NULL THEN 1 ELSE 0 END,
          CASE WHEN `exam_facility_id` IS NULL THEN 1 ELSE 0 END,
          `priority`,
          `precedence_rule_id`
        """,
        (case_row["event_id"], case_row["exam_facility_id"]),
    )
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in cur.fetchall():
        rule = dict(row)
        occurrence_no = rule.get("occurrence_no")
        if occurrence_no is None:
            grouped[(str(rule["namecode"]), 0)].append(rule)
        else:
            grouped[(str(rule["namecode"]), int(occurrence_no))].append(rule)
    return grouped


def value_text(row: dict[str, Any]) -> str:
    parts = [
        row.get("raw_value"),
        row.get("normalized_value"),
        row.get("code_display"),
        row.get("code_value"),
    ]
    return " ".join(str(part) for part in parts if part not in (None, ""))


def condition_matches(row: dict[str, Any] | None, condition_type: Any, pattern: Any) -> bool:
    if condition_type in (None, "", "ALWAYS"):
        return True
    condition = str(condition_type).upper()
    if row is None:
        return condition == "EMPTY"
    text = value_text(row)
    if condition == "NOT_EMPTY":
        return bool(text.strip())
    if condition == "EMPTY":
        return not text.strip()
    if condition == "REGEXP":
        return bool(pattern) and bool(re.search(str(pattern), text))
    if condition == "EQUALS":
        return text == str(pattern or "")
    if condition == "CONTAINS":
        return bool(pattern) and str(pattern) in text
    return False


def combine_text_values(xml_row: dict[str, Any] | None, csv_row: dict[str, Any] | None, separator: str) -> str:
    values: list[str] = []
    for row in (xml_row, csv_row):
        if row is None:
            continue
        text = value_text(row).strip()
        if text:
            values.append(text)
    return separator.join(values)


def make_joined_value(xml_row: dict[str, Any] | None, csv_row: dict[str, Any] | None, separator: str) -> dict[str, Any] | None:
    base = dict(xml_row or csv_row or {})
    if not base:
        return None
    joined = combine_text_values(xml_row, csv_row, separator)
    if not joined:
        return None
    base["normalized_value"] = joined
    base["normalized_unit"] = None
    base["nullflavor"] = None
    base["code_value"] = None
    base["code_display"] = None
    base["interpretation_code"] = None
    base["interpretation_name"] = None
    base["source_reference_lower"] = None
    base["source_reference_upper"] = None
    base["negation_ind"] = None
    base["source_type"] = "MERGED"
    base["source_role"] = "MERGED"
    return base


def choose_by_rule(
    candidates: list[dict[str, Any]],
    rules: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None, bool]:
    xml_candidate = next((row for row in candidates if row["source_type"] == "XML"), None)
    csv_candidate = next((row for row in candidates if row["source_type"] == "CSV"), None)
    for rule in rules:
        action = str(rule.get("action") or "").upper()
        if action == "XML_FIRST" and xml_candidate:
            return xml_candidate, "PRECEDENCE_XML_FIRST", True
        if action == "CSV_FIRST" and csv_candidate:
            return csv_candidate, "PRECEDENCE_CSV_FIRST", True
        if action == "CSV_IF_XML_MATCHES_PATTERN" and csv_candidate:
            if condition_matches(xml_candidate, rule.get("xml_value_condition_type"), rule.get("xml_value_condition_pattern")) and condition_matches(
                csv_candidate,
                rule.get("csv_value_condition_type"),
                rule.get("csv_value_condition_pattern"),
            ):
                return csv_candidate, "PRECEDENCE_CSV_IF_XML_MATCHES_PATTERN", True
        if action == "JOIN_XML_CSV":
            joined = make_joined_value(xml_candidate, csv_candidate, str(rule.get("join_separator") or "\n"))
            if joined is not None:
                return joined, "PRECEDENCE_JOIN_XML_CSV", True
        if action == "MANUAL_REVIEW":
            return None, "PRECEDENCE_MANUAL_REVIEW", True
    return None, None, False


def choose_default(candidates: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str | None]:
    xml_candidates = [row for row in candidates if row["source_type"] == "XML"]
    if xml_candidates:
        return xml_candidates[0], "XML_PRIMARY"
    csv_candidates = [row for row in candidates if row["source_type"] == "CSV"]
    if csv_candidates:
        return csv_candidates[0], "CSV_PRIMARY"
    manual_candidates = [row for row in candidates if row["source_type"] in {"PAPER", "MANUAL"}]
    if manual_candidates:
        return manual_candidates[0], "MANUAL_PRIMARY"
    return None, None


def selected_values(
    items: list[dict[str, Any]],
    rules: dict[tuple[str, int], list[dict[str, Any]]],
) -> tuple[list[tuple[dict[str, Any], str]], int, list[str]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    review_required: list[str] = []
    for item in items:
        if str(item.get("output_policy") or "").upper() == "REVIEW_REQUIRED":
            review_required.append(str(item["namecode"]))
            continue
        grouped[(str(item["namecode"]), int(item.get("occurrence_no") or 1))].append(item)
    selected: list[tuple[dict[str, Any], str]] = []
    rules_applied = 0
    for key, candidates in sorted(grouped.items()):
        namecode, occurrence_no = key
        rule_candidates = rules.get(key, []) + rules.get((namecode, 0), [])
        chosen, reason, applied = choose_by_rule(candidates, rule_candidates)
        if applied:
            rules_applied += 1
        if chosen is None and reason == "PRECEDENCE_MANUAL_REVIEW":
            continue
        if chosen is None:
            chosen, reason = choose_default(candidates)
        if chosen is not None and reason is not None:
            selected.append((chosen, reason))
    return selected, rules_applied, sorted(set(review_required))


VALUE_COLUMNS = [
    "exam_export_case_id",
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "namecode",
    "occurrence_no",
    "section_code",
    "section_code_system",
    "section_name",
    "normalized_value",
    "normalized_unit",
    "nullflavor",
    "code_value",
    "code_display",
    "interpretation_code",
    "interpretation_name",
    "source_reference_lower",
    "source_reference_upper",
    "negation_ind",
    "source_exam_item_value_id",
    "source_exam_ledger_id",
    "adopted_source_role",
    "adopted_reason",
    "built_etl_run_id",
    "built_at",
]


def insert_case_values(
    cur: Any,
    config: BuildValueConfig,
    *,
    case_row: dict[str, Any],
    selected: list[tuple[dict[str, Any], str]],
    run_id: int,
) -> int:
    if not selected:
        return 0
    columns_sql = ", ".join(f"`{column}`" for column in VALUE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(VALUE_COLUMNS))
    sql = f"""
        INSERT INTO {qname(config.health_db)}.`exam_export_case_values` ({columns_sql})
        VALUES ({placeholders})
    """
    now = datetime.now()
    rows = []
    for item, reason in selected:
        rows.append(
            (
                case_row["exam_export_case_id"],
                item.get("event_id"),
                item.get("subscriber_id"),
                item.get("hia_subscriber_id"),
                item.get("namecode"),
                item.get("occurrence_no") or 1,
                item.get("section_code"),
                item.get("section_code_system"),
                item.get("section_name"),
                item.get("normalized_value"),
                item.get("normalized_unit"),
                item.get("nullflavor"),
                item.get("code_value"),
                item.get("code_display"),
                item.get("interpretation_code"),
                item.get("interpretation_name"),
                item.get("source_reference_lower"),
                item.get("source_reference_upper"),
                item.get("negation_ind"),
                item.get("source_exam_item_value_id"),
                item.get("source_exam_ledger_id"),
                item.get("source_role"),
                reason,
                run_id,
                now,
            )
        )
    cur.executemany(sql, rows)
    return len(rows)


def clear_case_values(cur: Any, config: BuildValueConfig, case_id: int) -> int:
    cur.execute(
        f"DELETE FROM {qname(config.health_db)}.`exam_export_case_values` WHERE `exam_export_case_id` = %s",
        (case_id,),
    )
    return int(cur.rowcount or 0)


def clear_case_check_results(cur: Any, config: BuildValueConfig, case_id: int) -> int:
    cur.execute(
        f"""
        DELETE FROM {qname(config.health_db)}.`exam_check_results`
        WHERE `ledger_type` = 'EXPORT_CASE'
          AND `exam_export_case_id` = %s
        """,
        (case_id,),
    )
    return int(cur.rowcount or 0)


def update_case_value_status(
    cur: Any,
    config: BuildValueConfig,
    *,
    case_id: int,
    status: str,
    reason: str | None,
    count: int,
    run_id: int,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.`exam_export_cases`
        SET `value_build_status` = %s,
            `value_build_reason` = %s,
            `case_value_count` = %s,
            `check_status` = 'PENDING',
            `check_reason` = NULL,
            `manual_export_approved` = 0,
            `manual_export_reason` = NULL,
            `manual_export_approved_at` = NULL,
            `manual_export_approved_by` = NULL,
            `built_etl_run_id` = %s,
            `built_at` = CURRENT_TIMESTAMP(3),
            `updated_at` = CURRENT_TIMESTAMP(3)
        WHERE `exam_export_case_id` = %s
        """,
        (status, reason, count, run_id, case_id),
    )


def build_case_values(conn: Any, config: BuildValueConfig) -> BuildValueSummary:
    summary = BuildValueSummary(event_id=config.event_id, dry_run=config.dry_run)
    with dict_cursor(conn) as cur:
        run_id = 0
        if not config.dry_run:
            run_id = etl_start_run(
                cur,
                phase=ETL_PHASE,
                source=ETL_SOURCE,
                db_schema=config.health_db,
                db_path=config.health_db,
                input_base=f"event_id={config.event_id}",
                input_file=None,
                insurer_number=None,
                dry_run=config.dry_run,
                limit_rows=config.limit_cases or None,
            )
        cases = fetch_cases(cur, config)
        summary.cases_seen = len(cases)
        if config.dry_run:
            print(summary.message())
            return summary
        for case_row in cases:
            case_id = int(case_row["exam_export_case_id"])
            if case_row.get("merge_status") == "REVIEW_REQUIRED" and not config.include_review_required:
                summary.cases_skipped += 1
                continue
            items = fetch_case_items(cur, config, case_row)
            rules = fetch_precedence_rules(cur, config, case_row)
            selected, rules_applied, review_required = selected_values(items, rules)
            deleted = clear_case_values(cur, config, case_id)
            clear_case_check_results(cur, config, case_id)
            inserted = 0
            if not review_required:
                inserted = insert_case_values(cur, config, case_row=case_row, selected=selected, run_id=run_id)
            status = "READY" if inserted else "NO_VALUES"
            reason = None if inserted else "no valid source exam_item_values"
            if review_required:
                status = "REVIEW_REQUIRED"
                reason = "output policy review required: " + ",".join(review_required)
            update_case_value_status(cur, config, case_id=case_id, status=status, reason=reason, count=inserted, run_id=run_id)
            summary.values_deleted += deleted
            summary.values_inserted += inserted
            summary.precedence_rules_applied += rules_applied
            summary.cases_built += 1
        if config.case_ids:
            for case_id in config.case_ids:
                refresh_export_case_readiness(
                    cur,
                    health_db=config.health_db,
                    event_id=config.event_id,
                    exam_export_case_id=case_id,
                )
        else:
            refresh_export_case_readiness(cur, health_db=config.health_db, event_id=config.event_id)
        etl_finish_run(cur, run_id, summary.to_metrics(), extra_notes=summary.message())
    conn.commit()
    print(summary.message())
    return summary


def main() -> int:
    args = parse_args()
    config = BuildValueConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        master_db=args.master_db,
        dry_run=bool(args.dry_run),
        limit_cases=int(args.limit_cases or 0),
        include_review_required=bool(args.include_review_required),
        case_ids=tuple(args.case_id or ()),
    )
    validate_config(config)
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db) as conn:
        build_case_values(conn, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
