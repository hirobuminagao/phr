#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Refresh the report-only unified exam result ledger snapshot."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import start_run as etl_start_run


HEALTH_DB = "health_exam_result"
DEV_DB = "dev_phr"
WORK_DB = "work_other"
REPORT_TABLE = "exam_result_ledger_report"
ETL_PHASE = "REPORT_EXAM_RESULT_LEDGER"
ETL_SOURCE = "FROM_MEDICAL"


REPORT_COLUMNS = (
    "report_run_id",
    "ledger_type",
    "exam_ledger_id",
    "ledger_id",
    "event_id",
    "subscriber_id",
    "hia_subscriber_id",
    "xml_sha256",
    "xml_file_name",
    "document_id",
    "insurer_number",
    "facility_code",
    "facility_name",
    "exam_date",
    "name_kana_raw",
    "name_kana_match",
    "name_kana_export_value",
    "name_kana_export_source",
    "name_kana_export_reason",
    "insurance_symbol_raw",
    "insurance_symbol_match",
    "insurance_symbol_export_value",
    "insurance_symbol_export_source",
    "insurance_symbol_export_reason",
    "insurance_number_raw",
    "insurance_number_match",
    "insurance_number_export_value",
    "insurance_number_export_source",
    "insurance_number_export_reason",
    "birthdate",
    "gender_code",
    "report_category_code",
    "program_type_code",
    "identity_hash",
    "person_id_custom",
    "subscriber_match_status",
    "subscriber_match_method",
    "subscriber_match_reason",
    "basic_info_status",
    "basic_info_reason",
    "insurer_number_source",
    "insurer_number_completion_status",
    "insurer_number_completion_reason",
    "insurer_number_export_value",
    "address_source",
    "address_completion_status",
    "address_completion_reason",
    "address_completed_value",
    "postal_code_completed_value",
    "exam_item_status",
    "exam_item_reason",
    "xml_status",
    "xml_reason",
    "check_status",
    "check_reason",
    "xml_export_status",
    "manual_export_approved",
    "manual_export_reason",
    "source_created_at",
    "source_updated_at",
    "file_receipt_id",
    "source_etl_run_id",
    "src_row_no",
    "src_line_no",
    "row_sha256",
    "raw_row_json",
    "actual_header_sha256",
    "mapping_version",
    "exam_facility_id",
    "name_full_raw",
    "insurance_branch_number_raw",
    "insurance_branch_number_match",
    "gender_raw",
    "health_exam_report_category",
    "program_code",
    "postal_code",
    "address",
    "exam_facility_postal_code",
    "exam_facility_address",
    "exam_facility_phone_number",
    "exam_item_count",
    "exam_item_error_count",
    "row_status",
    "row_reason",
    "resume_approved",
    "resume_approved_at",
    "resume_approved_by",
    "resume_approved_reason",
    "manual_export_approved_at",
    "manual_export_approved_by",
    "correction_status",
    "merge_status",
    "merge_reason",
    "relationship_name",
    "qualification_lost_date",
    "hia_dashboard_status",
    "hia_dashboard_reservation_date",
    "hia_dashboard_exam_date",
    "hia_dashboard_medical_institution",
    "hia_dashboard_course_name",
    "refreshed_at",
)


@dataclass(frozen=True)
class ReportConfig:
    event_id: int
    health_db: str
    dev_db: str
    work_db: str
    dry_run: bool


@dataclass
class ReportSummary:
    event_id: int
    dry_run: bool
    xml_source_rows: int = 0
    csv_source_rows: int = 0
    paper_source_rows: int = 0
    existing_report_rows: int = 0
    deleted_rows: int = 0
    inserted_rows_count: int = 0

    @property
    def source_rows(self) -> int:
        return self.xml_source_rows + self.csv_source_rows + self.paper_source_rows

    @property
    def inserted_rows(self) -> int:
        return self.inserted_rows_count

    def to_message(self) -> str:
        return (
            f"exam_result_ledger_report event_id={self.event_id} "
            f"source_xml={self.xml_source_rows} source_csv={self.csv_source_rows} "
            f"source_paper={self.paper_source_rows} "
            f"existing={self.existing_report_rows} deleted={self.deleted_rows} "
            f"inserted={self.inserted_rows_count} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh a report table from unified exam_ledgers."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    parser.add_argument("--dev-db", default=DEV_DB)
    parser.add_argument("--work-db", default=WORK_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: ReportConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    qname(config.health_db)
    qname(config.dev_db)
    qname(config.work_db)


def source_count(cur: Any, *, schema: str, event_id: int, source_type: str) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qname(schema)}.`exam_ledgers`
        WHERE `event_id` = %s
          AND `source_type` = %s
        """,
        (event_id, source_type),
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def load_table_columns(cur: Any, *, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """,
        (schema, table),
    )
    return {str(row["COLUMN_NAME"]) for row in cur.fetchall()}


def build_insert_sql(
    config: ReportConfig,
    *,
    subscriber_columns: set[str],
    dashboard_columns: set[str],
) -> str:
    select_expressions: list[str] = []
    has_dashboard = bool(dashboard_columns)

    for target in REPORT_COLUMNS:
        if target == "report_run_id":
            expression = "%s"
        elif target == "ledger_type":
            expression = "l.`source_type`"
        elif target == "exam_ledger_id":
            expression = "l.`exam_ledger_id`"
        elif target == "ledger_id":
            expression = """
                CASE l.`source_type`
                    WHEN 'XML' THEN COALESCE(l.`source_xml_ledger_id`, l.`exam_ledger_id`)
                    WHEN 'CSV' THEN COALESCE(l.`source_csv_row_ledger_id`, l.`exam_ledger_id`)
                    ELSE l.`exam_ledger_id`
                END
            """
        elif target == "relationship_name":
            expression = "s.`relationship_name`" if "relationship_name" in subscriber_columns else "NULL"
        elif target == "qualification_lost_date":
            expression = "s.`qualification_lost_date`" if "qualification_lost_date" in subscriber_columns else "NULL"
        elif target == "hia_dashboard_status":
            expression = "hds.`status`" if has_dashboard and "status" in dashboard_columns else "NULL"
        elif target == "hia_dashboard_reservation_date":
            expression = "hds.`reservation_date`" if has_dashboard and "reservation_date" in dashboard_columns else "NULL"
        elif target == "hia_dashboard_exam_date":
            expression = "hds.`exam_date`" if has_dashboard and "exam_date" in dashboard_columns else "NULL"
        elif target == "hia_dashboard_medical_institution":
            expression = "hds.`medical_institution`" if has_dashboard and "medical_institution" in dashboard_columns else "NULL"
        elif target == "hia_dashboard_course_name":
            expression = "hds.`course_name`" if has_dashboard and "course_name" in dashboard_columns else "NULL"
        elif target == "refreshed_at":
            expression = "CURRENT_TIMESTAMP(3)"
        elif target == "source_created_at":
            expression = "COALESCE(l.`source_created_at`, l.`created_at`)"
        elif target == "source_updated_at":
            expression = "COALESCE(l.`source_updated_at`, l.`updated_at`)"
        else:
            expression = f"l.{qname(target)}"
        select_expressions.append(f"{expression} AS {qname(target)}")

    insert_columns = ",\n            ".join(qname(column) for column in REPORT_COLUMNS)
    select_columns = ",\n            ".join(select_expressions)
    dashboard_join = ""
    if has_dashboard:
        dashboard_join = f"""
        LEFT JOIN (
          SELECT
            `hia_subscriber_id`,
            MAX(`hia_dashboard_person_id`) AS `hia_dashboard_person_id`
          FROM {qname(config.work_db)}.`hia_dashboard_status`
          WHERE `is_active` = 1
            AND `hia_subscriber_id` IS NOT NULL
            AND `hia_subscriber_id` <> ''
          GROUP BY `hia_subscriber_id`
        ) AS hds_latest
          ON hds_latest.`hia_subscriber_id` = l.`hia_subscriber_id`
         AND l.`hia_subscriber_id` IS NOT NULL
         AND l.`hia_subscriber_id` <> ''
        LEFT JOIN {qname(config.work_db)}.`hia_dashboard_status` AS hds
          ON hds.`hia_dashboard_person_id` = hds_latest.`hia_dashboard_person_id`
        """
    return f"""
        INSERT INTO {qname(config.health_db)}.{qname(REPORT_TABLE)} (
            {insert_columns}
        )
        SELECT
            {select_columns}
        FROM {qname(config.health_db)}.`exam_ledgers` AS l
        LEFT JOIN {qname(config.dev_db)}.`subscribers` AS s
          ON s.`id` = l.`subscriber_id`
        {dashboard_join}
        WHERE l.`event_id` = %s
    """


def count_rows(cur: Any, *, schema: str, table: str, event_id: int) -> int:
    cur.execute(
        f"SELECT COUNT(*) AS cnt FROM {qname(schema)}.{qname(table)} WHERE `event_id` = %s",
        (event_id,),
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def delete_report_rows(cur: Any, config: ReportConfig) -> int:
    cur.execute(
        f"DELETE FROM {qname(config.health_db)}.{qname(REPORT_TABLE)} WHERE `event_id` = %s",
        (config.event_id,),
    )
    return int(cur.rowcount)


def insert_ledger_rows(
    cur: Any,
    *,
    config: ReportConfig,
    report_run_id: int,
) -> int:
    subscriber_columns = load_table_columns(cur, schema=config.dev_db, table="subscribers")
    dashboard_columns = load_table_columns(cur, schema=config.work_db, table="hia_dashboard_status")
    cur.execute(
        build_insert_sql(
            config,
            subscriber_columns=subscriber_columns,
            dashboard_columns=dashboard_columns,
        ),
        (report_run_id, config.event_id),
    )
    return int(cur.rowcount)


def load_summary(cur: Any, config: ReportConfig) -> ReportSummary:
    return ReportSummary(
        event_id=config.event_id,
        dry_run=config.dry_run,
        xml_source_rows=source_count(cur, schema=config.health_db, event_id=config.event_id, source_type="XML"),
        csv_source_rows=source_count(cur, schema=config.health_db, event_id=config.event_id, source_type="CSV"),
        paper_source_rows=source_count(
            cur,
            schema=config.health_db,
            event_id=config.event_id,
            source_type="PAPER",
        ),
        existing_report_rows=count_rows(
            cur,
            schema=config.health_db,
            table=REPORT_TABLE,
            event_id=config.event_id,
        ),
    )


def refresh_report(conn: Any, config: ReportConfig) -> ReportSummary:
    validate_config(config)
    cur = dict_cursor(conn)
    try:
        summary = load_summary(cur, config)
        if summary.source_rows == 0:
            raise RuntimeError(
                f"event_id={config.event_id} has no exam_ledger rows; run scan/import first"
            )
        if config.dry_run:
            return summary

        run_id = etl_start_run(
            cur,
            phase=ETL_PHASE,
            source=ETL_SOURCE,
            db_schema=config.health_db,
            db_path=config.health_db,
            input_base=f"event_id={config.event_id}",
            input_file=None,
            insurer_number=None,
            dry_run=False,
            limit_rows=None,
        )
        conn.commit()

        try:
            summary.deleted_rows = delete_report_rows(cur, config)
            summary.inserted_rows_count = insert_ledger_rows(
                cur,
                config=config,
                report_run_id=run_id,
            )
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(
                    rows_seen=summary.source_rows,
                    rows_inserted=summary.inserted_rows,
                ),
                status_override="success",
                extra_notes=summary.to_message(),
            )
            conn.commit()
            return summary
        except Exception as exc:
            conn.rollback()
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.source_rows, errors=1),
                status_override="failed",
                extra_notes=f"{summary.to_message()} error={type(exc).__name__}: {exc}",
            )
            conn.commit()
            raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = ReportConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        work_db=args.work_db,
        dry_run=bool(args.dry_run),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = refresh_report(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
