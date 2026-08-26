#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build person-level XML export cases from imported exam_ledgers."""

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
ETL_PHASE = "BUILD_EXAM_EXPORT_CASES"
ETL_SOURCE = "FROM_MEDICAL"


@dataclass(frozen=True)
class BuildCaseConfig:
    event_id: int
    health_db: str
    dev_db: str
    dry_run: bool
    limit_groups: int


@dataclass
class BuildCaseSummary:
    event_id: int
    dry_run: bool
    source_ledgers: int = 0
    candidate_groups: int = 0
    cases_inserted: int = 0
    cases_updated: int = 0
    sources_upserted: int = 0
    review_required: int = 0
    skipped: int = 0
    errors: int = 0

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=0,
            rows_seen=self.source_ledgers,
            rows_inserted=self.cases_inserted,
            rows_updated=self.cases_updated + self.sources_upserted,
            rows_skipped=self.skipped,
            errors=self.errors,
        )

    def message(self) -> str:
        return (
            f"build_exam_export_cases event_id={self.event_id} "
            f"sources={self.source_ledgers} groups={self.candidate_groups} "
            f"cases_inserted={self.cases_inserted} cases_updated={self.cases_updated} "
            f"sources_upserted={self.sources_upserted} review_required={self.review_required} "
            f"skipped={self.skipped} errors={self.errors} dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build person-level XML export cases from imported exam_ledgers.")
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-groups", type=int, default=0)
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    parser.add_argument("--dev-db", default="dev_phr")
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: BuildCaseConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    if config.limit_groups < 0:
        raise ValueError("limit_groups must be >= 0")
    qname(config.health_db)
    qname(config.dev_db)


def case_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("event_id"),
        row.get("subscriber_id"),
        row.get("exam_date"),
        row.get("exam_facility_id"),
        row.get("insurer_number"),
    )


def fetch_source_ledgers(cur: Any, config: BuildCaseConfig) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
          el.*,
          d.`exam_export_case_id` AS manual_exam_export_case_id,
          COALESCE(
            el.`subscriber_id`,
            eec.`subscriber_id`,
            (
              SELECT s.`id`
              FROM {qname(config.dev_db)}.`subscribers` AS s
              WHERE el.`source_type` IN ('PAPER', 'MANUAL')
                AND el.`hia_subscriber_id` IS NOT NULL
                AND el.`hia_subscriber_id` <> ''
                AND s.`hia_subscriber_id` = el.`hia_subscriber_id`
              ORDER BY s.`id` DESC
              LIMIT 1
            ),
            (
              SELECT s.`id`
              FROM {qname(config.dev_db)}.`subscribers` AS s
              WHERE el.`source_type` IN ('PAPER', 'MANUAL')
                AND el.`identity_hash` IS NOT NULL
                AND el.`identity_hash` <> ''
                AND s.`identity_hash` = el.`identity_hash`
              ORDER BY s.`id` DESC
              LIMIT 1
            ),
            (
              SELECT s.`id`
              FROM {qname(config.dev_db)}.`subscribers` AS s
              WHERE el.`source_type` IN ('PAPER', 'MANUAL')
                AND el.`person_id_custom` IS NOT NULL
                AND el.`person_id_custom` <> ''
                AND s.`person_id_custom` = el.`person_id_custom`
                AND (
                  el.`name_kana_match` IS NULL
                  OR el.`name_kana_match` = ''
                  OR s.`name_kana_full_match` = el.`name_kana_match`
                )
              ORDER BY s.`id` DESC
              LIMIT 1
            )
          ) AS resolved_subscriber_id,
          COALESCE(el.`exam_date`, eec.`exam_date`) AS resolved_exam_date,
          COALESCE(el.`exam_facility_id`, eec.`exam_facility_id`) AS resolved_exam_facility_id,
          COALESCE(el.`insurer_number`, eec.`insurer_number`) AS resolved_insurer_number,
          COALESCE(el.`facility_code`, eec.`facility_code`) AS resolved_facility_code,
          COALESCE(el.`facility_name`, eec.`facility_name`) AS resolved_facility_name
        FROM {qname(config.health_db)}.`exam_ledgers` AS el
        LEFT JOIN {qname(config.health_db)}.`manual_exam_entry_drafts` AS d
          ON el.`source_type` IN ('PAPER', 'MANUAL')
         AND CAST(d.`manual_exam_entry_draft_id` AS CHAR) = JSON_UNQUOTE(JSON_EXTRACT(el.`raw_row_json`, '$.manual_exam_entry_draft_id'))
        LEFT JOIN {qname(config.health_db)}.`exam_export_cases` AS eec
          ON eec.`exam_export_case_id` = d.`exam_export_case_id`
        WHERE el.`event_id` = %s
          AND el.`source_type` IN ('XML', 'CSV', 'PAPER', 'MANUAL')
          AND COALESCE(el.`row_status`, '') <> 'REVERTED_TO_DRAFT'
          AND (
            el.`subscriber_match_status` = 'MATCHED'
            OR (
              el.`source_type` IN ('PAPER', 'MANUAL')
              AND el.`subscriber_match_status` IN ('MANUAL_ENTRY', 'MANUAL_CONFIRMED')
            )
          )
        HAVING resolved_subscriber_id IS NOT NULL
          AND resolved_exam_date IS NOT NULL
          AND resolved_exam_facility_id IS NOT NULL
          AND resolved_insurer_number IS NOT NULL
        ORDER BY resolved_subscriber_id, resolved_exam_date, resolved_exam_facility_id, resolved_insurer_number, el.`source_type`, el.`exam_ledger_id`
        """,
        (config.event_id,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        if row.get("subscriber_id") is None and row.get("resolved_subscriber_id") is not None:
            row["subscriber_id"] = row["resolved_subscriber_id"]
        if row.get("manual_exam_export_case_id"):
            row["exam_date"] = row.get("resolved_exam_date")
            row["exam_facility_id"] = row.get("resolved_exam_facility_id")
            row["insurer_number"] = row.get("resolved_insurer_number")
            row["facility_code"] = row.get("resolved_facility_code")
            row["facility_name"] = row.get("resolved_facility_name")
    return rows


def select_groups(rows: Iterable[dict[str, Any]], limit: int) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[case_key(row)].append(row)
    groups = sorted(
        grouped.values(),
        key=lambda group: (
            group[0].get("subscriber_id"),
            group[0].get("exam_date"),
            group[0].get("exam_facility_id"),
            group[0].get("insurer_number"),
        ),
    )
    return groups[:limit] if limit else groups


def choose_primary(group: list[dict[str, Any]]) -> dict[str, Any]:
    xml_rows = [row for row in group if row["source_type"] == "XML"]
    if xml_rows:
        return sorted(xml_rows, key=lambda row: (row.get("check_status") != "OK", row["exam_ledger_id"]))[0]
    return sorted(group, key=lambda row: (row.get("check_status") != "OK", row["exam_ledger_id"]))[0]


def source_mode(group: list[dict[str, Any]]) -> str:
    xml_count = sum(1 for row in group if row["source_type"] == "XML")
    csv_count = sum(1 for row in group if row["source_type"] == "CSV")
    paper_count = sum(1 for row in group if row["source_type"] in {"PAPER", "MANUAL"})
    if xml_count == 1 and csv_count == 0 and paper_count == 0:
        return "XML_ONLY"
    if xml_count == 0 and csv_count == 1 and paper_count == 0:
        return "CSV_ONLY"
    if xml_count == 0 and csv_count == 0 and paper_count == 1:
        return "PAPER_ONLY"
    if xml_count == 1 and csv_count == 1 and paper_count == 0:
        return "XML_CSV"
    if xml_count == 0 and csv_count == 1 and paper_count == 1:
        return "CSV_PAPER"
    if xml_count == 1 and csv_count == 0 and paper_count == 1:
        return "XML_PAPER"
    if xml_count == 1 and csv_count == 1 and paper_count == 1:
        return "XML_CSV_PAPER"
    if paper_count >= 1:
        return "MULTI_WITH_PAPER"
    return "MULTI_SOURCE"


def merge_status(group: list[dict[str, Any]]) -> tuple[str, str | None]:
    mode = source_mode(group)
    if mode in {"XML_ONLY", "CSV_ONLY", "PAPER_ONLY"}:
        return "SOURCE_SINGLE", mode
    if mode == "XML_CSV":
        return "READY", "XML primary with CSV supplement candidate"
    if mode == "CSV_PAPER":
        return "READY", "CSV primary with paper supplement candidate"
    if mode == "XML_PAPER":
        return "READY", "XML primary with paper supplement candidate"
    if mode == "XML_CSV_PAPER":
        return "READY", "XML primary with CSV and paper supplement candidates"
    if mode == "MULTI_WITH_PAPER":
        return "READY", "multiple source ledgers with paper supplement candidate"
    return "REVIEW_REQUIRED", "multiple source ledgers require manual review"


def case_params(primary: dict[str, Any], group: list[dict[str, Any]], run_id: int) -> dict[str, Any]:
    mode = source_mode(group)
    merge, reason = merge_status(group)
    return {
        "event_id": primary.get("event_id"),
        "subscriber_id": primary.get("subscriber_id"),
        "hia_subscriber_id": primary.get("hia_subscriber_id"),
        "identity_hash": primary.get("identity_hash"),
        "person_id_custom": primary.get("person_id_custom"),
        "subscriber_match_status": primary.get("subscriber_match_status") or "MATCHED",
        "subscriber_match_reason": primary.get("subscriber_match_reason"),
        "insurer_number": primary.get("insurer_number"),
        "exam_facility_id": primary.get("exam_facility_id"),
        "facility_code": primary.get("facility_code"),
        "facility_name": primary.get("facility_name"),
        "exam_date": primary.get("exam_date"),
        "exam_date_export_value": primary.get("exam_date_export_value"),
        "exam_date_export_source": primary.get("exam_date_export_source"),
        "exam_date_export_reason": primary.get("exam_date_export_reason"),
        "health_exam_report_category": primary.get("health_exam_report_category") or primary.get("report_category_code"),
        "program_code": primary.get("program_code") or primary.get("program_type_code"),
        "name_full_raw": primary.get("name_full_raw"),
        "name_kana_raw": primary.get("name_kana_raw"),
        "name_kana_export_value": primary.get("name_kana_export_value"),
        "name_kana_export_source": primary.get("name_kana_export_source"),
        "name_kana_export_reason": primary.get("name_kana_export_reason"),
        "insurance_symbol_raw": primary.get("insurance_symbol_raw"),
        "insurance_symbol_export_value": primary.get("insurance_symbol_export_value"),
        "insurance_symbol_export_source": primary.get("insurance_symbol_export_source"),
        "insurance_symbol_export_reason": primary.get("insurance_symbol_export_reason"),
        "insurance_number_raw": primary.get("insurance_number_raw"),
        "insurance_number_export_value": primary.get("insurance_number_export_value"),
        "insurance_number_export_source": primary.get("insurance_number_export_source"),
        "insurance_number_export_reason": primary.get("insurance_number_export_reason"),
        "insurance_branch_number_raw": primary.get("insurance_branch_number_raw"),
        "insurance_branch_number_export_value": primary.get("insurance_branch_number_export_value"),
        "insurance_branch_number_export_source": primary.get("insurance_branch_number_export_source"),
        "insurance_branch_number_export_reason": primary.get("insurance_branch_number_export_reason"),
        "exam_ticket_number_export_value": primary.get("exam_ticket_number_export_value"),
        "exam_ticket_number_export_source": primary.get("exam_ticket_number_export_source"),
        "exam_ticket_number_export_reason": primary.get("exam_ticket_number_export_reason"),
        "exam_ticket_expires_on_export_value": primary.get("exam_ticket_expires_on_export_value"),
        "exam_ticket_expires_on_export_source": primary.get("exam_ticket_expires_on_export_source"),
        "exam_ticket_expires_on_export_reason": primary.get("exam_ticket_expires_on_export_reason"),
        "birthdate": primary.get("birthdate"),
        "gender_code": primary.get("gender_code"),
        "gender_raw": primary.get("gender_raw"),
        "postal_code": primary.get("postal_code"),
        "address": primary.get("address"),
        "insurer_number_export_value": primary.get("insurer_number_export_value"),
        "address_source": primary.get("address_source"),
        "address_completion_status": primary.get("address_completion_status"),
        "address_completion_reason": primary.get("address_completion_reason"),
        "address_completed_value": primary.get("address_completed_value"),
        "postal_code_completed_value": primary.get("postal_code_completed_value"),
        "exam_facility_postal_code": primary.get("exam_facility_postal_code"),
        "exam_facility_address": primary.get("exam_facility_address"),
        "exam_facility_phone_number": primary.get("exam_facility_phone_number"),
        "source_mode": mode,
        "case_status": "READY" if merge != "REVIEW_REQUIRED" else "REVIEW_REQUIRED",
        "case_reason": reason,
        "merge_status": merge,
        "merge_reason": reason,
        "built_etl_run_id": run_id,
        "built_at": datetime.now(),
    }


CASE_COLUMNS = [
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "identity_hash",
    "person_id_custom",
    "subscriber_match_status",
    "subscriber_match_reason",
    "insurer_number",
    "exam_facility_id",
    "facility_code",
    "facility_name",
    "exam_date",
    "exam_date_export_value",
    "exam_date_export_source",
    "exam_date_export_reason",
    "health_exam_report_category",
    "program_code",
    "name_full_raw",
    "name_kana_raw",
    "name_kana_export_value",
    "name_kana_export_source",
    "name_kana_export_reason",
    "insurance_symbol_raw",
    "insurance_symbol_export_value",
    "insurance_symbol_export_source",
    "insurance_symbol_export_reason",
    "insurance_number_raw",
    "insurance_number_export_value",
    "insurance_number_export_source",
    "insurance_number_export_reason",
    "insurance_branch_number_raw",
    "insurance_branch_number_export_value",
    "insurance_branch_number_export_source",
    "insurance_branch_number_export_reason",
    "exam_ticket_number_export_value",
    "exam_ticket_number_export_source",
    "exam_ticket_number_export_reason",
    "exam_ticket_expires_on_export_value",
    "exam_ticket_expires_on_export_source",
    "exam_ticket_expires_on_export_reason",
    "birthdate",
    "gender_code",
    "gender_raw",
    "postal_code",
    "address",
    "insurer_number_export_value",
    "address_source",
    "address_completion_status",
    "address_completion_reason",
    "address_completed_value",
    "postal_code_completed_value",
    "exam_facility_postal_code",
    "exam_facility_address",
    "exam_facility_phone_number",
    "source_mode",
    "case_status",
    "case_reason",
    "merge_status",
    "merge_reason",
    "built_etl_run_id",
    "built_at",
]


BASIC_INFO_CORRECTION_CASE_COLUMNS = {
    "exam_date": ("exam_date_export_value", "exam_date_export_source", "exam_date_export_reason"),
    "name_kana": ("name_kana_export_value", "name_kana_export_source", "name_kana_export_reason"),
    "insurance_symbol": (
        "insurance_symbol_export_value",
        "insurance_symbol_export_source",
        "insurance_symbol_export_reason",
    ),
    "insurance_number": (
        "insurance_number_export_value",
        "insurance_number_export_source",
        "insurance_number_export_reason",
    ),
    "insurance_branch_number": (
        "insurance_branch_number_export_value",
        "insurance_branch_number_export_source",
        "insurance_branch_number_export_reason",
    ),
    "exam_ticket_number": (
        "exam_ticket_number_export_value",
        "exam_ticket_number_export_source",
        "exam_ticket_number_export_reason",
    ),
    "exam_ticket_expires_on": (
        "exam_ticket_expires_on_export_value",
        "exam_ticket_expires_on_export_source",
        "exam_ticket_expires_on_export_reason",
    ),
    "insurer_number": ("insurer_number_export_value", None, None),
    "postal_code": ("postal_code_completed_value", None, "address_completion_reason"),
    "address": ("address_completed_value", "address_source", "address_completion_reason"),
}


def reapply_basic_info_corrections(cur: Any, config: BuildCaseConfig, *, case_id: int) -> None:
    cur.execute(
        f"""
        SELECT field_code, normalized_value, correction_reason
        FROM {qname(config.health_db)}.`exam_case_basic_info_corrections`
        WHERE exam_export_case_id = %s
          AND correction_status = 'ACTIVE'
        ORDER BY exam_case_basic_info_correction_id
        """,
        (case_id,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    if not rows:
        return
    for row in rows:
        field_code = str(row.get("field_code") or "")
        columns = BASIC_INFO_CORRECTION_CASE_COLUMNS.get(field_code)
        if not columns:
            continue
        value_column, source_column, reason_column = columns
        update_columns = [f"`{value_column}` = %s", "`correction_status` = 'CORRECTED'"]
        params: list[Any] = [row.get("normalized_value")]
        if source_column:
            update_columns.append(f"`{source_column}` = 'MANUAL_CORRECTION'")
        if reason_column:
            update_columns.append(f"`{reason_column}` = %s")
            params.append(row.get("correction_reason") or "MANUAL_CORRECTION")
        if field_code in {"postal_code", "address"}:
            update_columns.append("`address_completion_status` = 'MANUAL_CORRECTION'")
        params.append(case_id)
        cur.execute(
            f"""
            UPDATE {qname(config.health_db)}.`exam_export_cases`
            SET {", ".join(update_columns)}
            WHERE exam_export_case_id = %s
            """,
            tuple(params),
        )


def upsert_case(cur: Any, config: BuildCaseConfig, params: dict[str, Any]) -> tuple[int, str]:
    columns_sql = ", ".join(f"`{column}`" for column in CASE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(CASE_COLUMNS))
    update_columns = [column for column in CASE_COLUMNS if column not in {"event_id", "subscriber_id", "exam_date", "exam_facility_id", "insurer_number"}]
    update_sql = ", ".join(f"`{column}` = VALUES(`{column}`)" for column in update_columns)
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.`exam_export_cases` ({columns_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
          {update_sql},
          `updated_at` = CURRENT_TIMESTAMP(3)
        """,
        tuple(params[column] for column in CASE_COLUMNS),
    )
    action = "inserted" if int(cur.rowcount or 0) == 1 else "updated"
    if action == "inserted" and cur.lastrowid:
        return int(cur.lastrowid), action
    cur.execute(
        f"""
        SELECT `exam_export_case_id`
        FROM {qname(config.health_db)}.`exam_export_cases`
        WHERE `event_id` = %s
          AND `subscriber_id` = %s
          AND `exam_date` = %s
          AND `exam_facility_id` = %s
          AND `insurer_number` = %s
        """,
        (
            params["event_id"],
            params["subscriber_id"],
            params["exam_date"],
            params["exam_facility_id"],
            params["insurer_number"],
        ),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("failed to resolve exam_export_case_id after upsert")
    return int(row["exam_export_case_id"]), action


def source_role(row: dict[str, Any], primary: dict[str, Any], group: list[dict[str, Any]]) -> tuple[int, str, str]:
    if int(row["exam_ledger_id"]) == int(primary["exam_ledger_id"]):
        return 10, "PRIMARY", "primary source for case"
    if row["source_type"] == "CSV" and any(item["source_type"] == "XML" for item in group):
        return 20, "SUPPLEMENT", "CSV supplement candidate for XML primary case"
    return 50, "SUPPLEMENT", "additional source candidate"


def upsert_sources(
    cur: Any,
    config: BuildCaseConfig,
    *,
    case_id: int,
    group: list[dict[str, Any]],
    primary: dict[str, Any],
) -> int:
    changed = 0
    for row in group:
        priority, role, reason = source_role(row, primary, group)
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.`exam_export_case_sources` (
                `exam_export_case_id`, `source_exam_ledger_id`, `source_type`,
                `file_receipt_id`, `source_priority`, `source_role`, `source_status`, `source_reason`
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s)
            ON DUPLICATE KEY UPDATE
                `exam_export_case_id` = VALUES(`exam_export_case_id`),
                `source_type` = VALUES(`source_type`),
                `file_receipt_id` = VALUES(`file_receipt_id`),
                `source_priority` = VALUES(`source_priority`),
                `source_role` = VALUES(`source_role`),
                `source_status` = VALUES(`source_status`),
                `source_reason` = VALUES(`source_reason`),
                `updated_at` = CURRENT_TIMESTAMP(3)
            """,
            (
                case_id,
                row["exam_ledger_id"],
                row["source_type"],
                row.get("file_receipt_id"),
                priority,
                role,
                reason,
            ),
        )
        changed += 1
    return changed


def build_cases(conn: Any, config: BuildCaseConfig) -> BuildCaseSummary:
    summary = BuildCaseSummary(event_id=config.event_id, dry_run=config.dry_run)
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
                limit_rows=config.limit_groups or None,
            )
        rows = fetch_source_ledgers(cur, config)
        groups = select_groups(rows, config.limit_groups)
        summary.source_ledgers = len(rows)
        summary.candidate_groups = len(groups)
        if config.dry_run:
            summary.review_required = sum(1 for group in groups if merge_status(group)[0] == "REVIEW_REQUIRED")
            print(summary.message())
            return summary

        for group in groups:
            primary = choose_primary(group)
            params = case_params(primary, group, run_id)
            case_id, action = upsert_case(cur, config, params)
            if action == "inserted":
                summary.cases_inserted += 1
            else:
                summary.cases_updated += 1
            if params["merge_status"] == "REVIEW_REQUIRED":
                summary.review_required += 1
            summary.sources_upserted += upsert_sources(cur, config, case_id=case_id, group=group, primary=primary)
            reapply_basic_info_corrections(cur, config, case_id=case_id)

        refresh_export_case_readiness(cur, health_db=config.health_db, event_id=config.event_id)
        etl_finish_run(cur, run_id, summary.to_metrics(), extra_notes=summary.message())
    conn.commit()
    print(summary.message())
    return summary


def main() -> int:
    args = parse_args()
    config = BuildCaseConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
        limit_groups=int(args.limit_groups or 0),
    )
    validate_config(config)
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db) as conn:
        build_cases(conn, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
