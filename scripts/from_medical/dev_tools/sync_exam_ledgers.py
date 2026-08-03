#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync xml_ledger/csv_row_ledger into the unified exam_ledgers tables."""

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
ETL_PHASE = "SYNC_EXAM_LEDGERS"
ETL_SOURCE = "FROM_MEDICAL"


@dataclass(frozen=True)
class SyncConfig:
    event_id: int
    health_db: str
    dry_run: bool


@dataclass
class SyncSummary:
    event_id: int
    dry_run: bool
    xml_source_rows: int = 0
    csv_source_rows: int = 0
    existing_exam_ledgers: int = 0
    xml_upserted_rows: int = 0
    csv_upserted_rows: int = 0
    xml_source_upserted_rows: int = 0
    csv_source_upserted_rows: int = 0

    @property
    def source_rows(self) -> int:
        return self.xml_source_rows + self.csv_source_rows

    @property
    def changed_rows(self) -> int:
        return (
            self.xml_upserted_rows
            + self.csv_upserted_rows
            + self.xml_source_upserted_rows
            + self.csv_source_upserted_rows
        )

    def to_message(self) -> str:
        return (
            f"sync_exam_ledgers event_id={self.event_id} "
            f"source_xml={self.xml_source_rows} source_csv={self.csv_source_rows} "
            f"existing={self.existing_exam_ledgers} "
            f"upsert_xml={self.xml_upserted_rows} upsert_csv={self.csv_upserted_rows} "
            f"source_xml={self.xml_source_upserted_rows} source_csv={self.csv_source_upserted_rows} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync XML/CSV source ledgers into health_exam_result.exam_ledgers."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: SyncConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    qname(config.health_db)


def count_rows(cur: Any, *, schema: str, table: str, event_id: int) -> int:
    cur.execute(
        f"SELECT COUNT(*) AS cnt FROM {qname(schema)}.{qname(table)} WHERE `event_id` = %s",
        (event_id,),
    )
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def load_summary(cur: Any, config: SyncConfig) -> SyncSummary:
    return SyncSummary(
        event_id=config.event_id,
        dry_run=config.dry_run,
        xml_source_rows=count_rows(cur, schema=config.health_db, table="xml_ledger", event_id=config.event_id),
        csv_source_rows=count_rows(cur, schema=config.health_db, table="csv_row_ledger", event_id=config.event_id),
        existing_exam_ledgers=count_rows(
            cur,
            schema=config.health_db,
            table="exam_ledgers",
            event_id=config.event_id,
        ),
    )


def upsert_xml_ledgers(cur: Any, config: SyncConfig) -> int:
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.`exam_ledgers` (
            `event_id`, `source_type`, `source_xml_ledger_id`,
            `file_receipt_id`,
            `subscriber_id`, `hia_subscriber_id`, `identity_hash`, `person_id_custom`,
            `subscriber_match_status`, `subscriber_match_method`, `subscriber_match_reason`,
            `xml_sha256`, `xml_file_name`, `document_id`,
            `insurer_number`, `facility_code`, `facility_name`, `exam_date`,
            `name_kana_raw`, `name_kana_match`,
            `insurance_symbol_raw`, `insurance_symbol_match`,
            `insurance_number_raw`, `insurance_number_match`,
            `birthdate`, `gender_code`,
            `report_category_code`, `program_type_code`,
            `health_exam_report_category`, `program_code`,
            `exam_item_status`, `exam_item_reason`,
            `xml_status`, `xml_reason`,
            `check_status`, `check_reason`, `xml_export_status`,
            `manual_export_approved`, `manual_export_reason`,
            `merge_status`, `source_created_at`, `source_updated_at`
        )
        SELECT
            l.`event_id`, 'XML', l.`id`,
            link.`file_receipt_id`,
            l.`subscriber_id`, l.`hia_subscriber_id`, l.`identity_hash`, l.`person_id_custom`,
            l.`subscriber_match_status`, l.`subscriber_match_method`, l.`subscriber_match_reason`,
            l.`xml_sha256`, l.`xml_file_name`, l.`document_id`,
            l.`insurer_number`, l.`facility_code`, l.`facility_name`, l.`exam_date`,
            l.`name_kana_raw`, l.`name_kana_match`,
            l.`insurance_symbol_raw`, l.`insurance_symbol_match`,
            l.`insurance_number_raw`, l.`insurance_number_match`,
            l.`birthdate`, l.`gender_code`,
            l.`report_category_code`, l.`program_type_code`,
            l.`report_category_code`, l.`program_type_code`,
            l.`exam_item_status`, l.`exam_item_reason`,
            l.`xml_status`, l.`xml_reason`,
            l.`check_status`, l.`check_reason`,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM {qname(config.health_db)}.`xml_export_members` AS xem
                WHERE xem.`event_id` = l.`event_id`
                  AND xem.`ledger_type` = 'XML'
                  AND xem.`ledger_id` = l.`id`
              ) THEN 'EXPORTED'
              ELSE l.`xml_export_status`
            END,
            l.`manual_export_approved`, l.`manual_export_reason`,
            'SOURCE_SINGLE', l.`created_at`, l.`updated_at`
        FROM {qname(config.health_db)}.`xml_ledger` AS l
        LEFT JOIN (
            SELECT
                `xml_ledger_id`,
                MIN(`file_receipt_id`) AS `file_receipt_id`
            FROM {qname(config.health_db)}.`xml_file_links`
            GROUP BY `xml_ledger_id`
        ) AS link
          ON link.`xml_ledger_id` = l.`id`
        WHERE l.`event_id` = %s
        ON DUPLICATE KEY UPDATE
            `file_receipt_id` = VALUES(`file_receipt_id`),
            `subscriber_id` = VALUES(`subscriber_id`),
            `hia_subscriber_id` = VALUES(`hia_subscriber_id`),
            `identity_hash` = VALUES(`identity_hash`),
            `person_id_custom` = VALUES(`person_id_custom`),
            `subscriber_match_status` = VALUES(`subscriber_match_status`),
            `subscriber_match_method` = VALUES(`subscriber_match_method`),
            `subscriber_match_reason` = VALUES(`subscriber_match_reason`),
            `xml_sha256` = VALUES(`xml_sha256`),
            `xml_file_name` = VALUES(`xml_file_name`),
            `document_id` = VALUES(`document_id`),
            `insurer_number` = VALUES(`insurer_number`),
            `facility_code` = VALUES(`facility_code`),
            `facility_name` = VALUES(`facility_name`),
            `exam_date` = VALUES(`exam_date`),
            `name_kana_raw` = VALUES(`name_kana_raw`),
            `name_kana_match` = VALUES(`name_kana_match`),
            `insurance_symbol_raw` = VALUES(`insurance_symbol_raw`),
            `insurance_symbol_match` = VALUES(`insurance_symbol_match`),
            `insurance_number_raw` = VALUES(`insurance_number_raw`),
            `insurance_number_match` = VALUES(`insurance_number_match`),
            `birthdate` = VALUES(`birthdate`),
            `gender_code` = VALUES(`gender_code`),
            `report_category_code` = VALUES(`report_category_code`),
            `program_type_code` = VALUES(`program_type_code`),
            `health_exam_report_category` = VALUES(`health_exam_report_category`),
            `program_code` = VALUES(`program_code`),
            `exam_item_status` = VALUES(`exam_item_status`),
            `exam_item_reason` = VALUES(`exam_item_reason`),
            `xml_status` = VALUES(`xml_status`),
            `xml_reason` = VALUES(`xml_reason`),
            `check_status` = VALUES(`check_status`),
            `check_reason` = VALUES(`check_reason`),
            `xml_export_status` = CASE
                WHEN {qname(config.health_db)}.`exam_ledgers`.`xml_export_status` = 'EXPORTED'
                    THEN {qname(config.health_db)}.`exam_ledgers`.`xml_export_status`
                WHEN VALUES(`xml_export_status`) = 'EXPORTED' THEN VALUES(`xml_export_status`)
                ELSE VALUES(`xml_export_status`)
            END,
            `manual_export_approved` = VALUES(`manual_export_approved`),
            `manual_export_reason` = VALUES(`manual_export_reason`),
            `source_updated_at` = VALUES(`source_updated_at`)
        """,
        (config.event_id,),
    )
    return int(cur.rowcount)


def upsert_csv_ledgers(cur: Any, config: SyncConfig) -> int:
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.`exam_ledgers` (
            `event_id`, `source_type`, `source_csv_row_ledger_id`,
            `file_receipt_id`, `source_etl_run_id`, `src_row_no`, `src_line_no`,
            `row_sha256`, `raw_row_json`, `actual_header_sha256`, `mapping_version`,
            `subscriber_id`, `hia_subscriber_id`, `identity_hash`, `person_id_custom`,
            `subscriber_match_status`, `subscriber_match_method`, `subscriber_match_reason`,
            `insurer_number`, `exam_facility_id`, `facility_code`, `facility_name`, `exam_date`,
            `name_full_raw`, `name_kana_raw`, `name_kana_match`,
            `insurance_symbol_raw`, `insurance_symbol_match`,
            `insurance_number_raw`, `insurance_number_match`,
            `insurance_branch_number_raw`, `insurance_branch_number_match`,
            `birthdate`, `gender_code`, `gender_raw`,
            `health_exam_report_category`, `program_code`,
            `postal_code`, `address`,
            `exam_facility_postal_code`, `exam_facility_address`, `exam_facility_phone_number`,
            `exam_item_status`, `exam_item_count`, `exam_item_error_count`, `exam_item_reason`,
            `row_status`, `row_reason`,
            `check_status`, `check_reason`, `xml_export_status`,
            `manual_export_approved`, `manual_export_reason`,
            `manual_export_approved_at`, `manual_export_approved_by`,
            `resume_approved`, `resume_approved_at`, `resume_approved_by`, `resume_approved_reason`,
            `merge_status`, `source_created_at`, `source_updated_at`
        )
        SELECT
            l.`event_id`, 'CSV', l.`csv_row_ledger_id`,
            l.`file_receipt_id`, l.`etl_run_id`, l.`src_row_no`, l.`src_line_no`,
            l.`row_sha256`, l.`raw_row_json`, l.`actual_header_sha256`, l.`mapping_version`,
            l.`subscriber_id`, l.`hia_subscriber_id`, l.`identity_hash`, l.`person_id_custom`,
            l.`subscriber_match_status`, l.`subscriber_match_method`, l.`subscriber_match_reason`,
            l.`insurer_number`, l.`exam_facility_id`, l.`facility_code`, l.`facility_name`, l.`exam_date`,
            l.`name_full_raw`, l.`name_kana_raw`, l.`name_kana_match`,
            l.`insurance_symbol_raw`, l.`insurance_symbol_match`,
            l.`insurance_number_raw`, l.`insurance_number_match`,
            l.`insurance_branch_number_raw`, l.`insurance_branch_number_match`,
            l.`birthdate`, l.`gender_code`, l.`gender_raw`,
            l.`health_exam_report_category`, l.`program_code`,
            l.`postal_code`, l.`address`,
            l.`exam_facility_postal_code`, l.`exam_facility_address`, l.`exam_facility_phone_number`,
            l.`exam_item_status`, l.`exam_item_count`, l.`exam_item_error_count`, l.`exam_item_reason`,
            l.`row_status`, l.`row_reason`,
            l.`check_status`, l.`check_reason`,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM {qname(config.health_db)}.`xml_export_members` AS xem
                WHERE xem.`event_id` = l.`event_id`
                  AND xem.`ledger_type` = 'CSV'
                  AND xem.`ledger_id` = l.`csv_row_ledger_id`
              ) THEN 'EXPORTED'
              ELSE l.`xml_export_status`
            END,
            l.`manual_export_approved`, l.`manual_export_reason`,
            l.`manual_export_approved_at`, l.`manual_export_approved_by`,
            l.`resume_approved`, l.`resume_approved_at`, l.`resume_approved_by`, l.`resume_approved_reason`,
            'SOURCE_SINGLE', l.`created_at`, l.`updated_at`
        FROM {qname(config.health_db)}.`csv_row_ledger` AS l
        WHERE l.`event_id` = %s
        ON DUPLICATE KEY UPDATE
            `file_receipt_id` = VALUES(`file_receipt_id`),
            `source_etl_run_id` = VALUES(`source_etl_run_id`),
            `src_row_no` = VALUES(`src_row_no`),
            `src_line_no` = VALUES(`src_line_no`),
            `row_sha256` = VALUES(`row_sha256`),
            `raw_row_json` = VALUES(`raw_row_json`),
            `actual_header_sha256` = VALUES(`actual_header_sha256`),
            `mapping_version` = VALUES(`mapping_version`),
            `subscriber_id` = VALUES(`subscriber_id`),
            `hia_subscriber_id` = VALUES(`hia_subscriber_id`),
            `identity_hash` = VALUES(`identity_hash`),
            `person_id_custom` = VALUES(`person_id_custom`),
            `subscriber_match_status` = VALUES(`subscriber_match_status`),
            `subscriber_match_method` = VALUES(`subscriber_match_method`),
            `subscriber_match_reason` = VALUES(`subscriber_match_reason`),
            `insurer_number` = VALUES(`insurer_number`),
            `exam_facility_id` = VALUES(`exam_facility_id`),
            `facility_code` = VALUES(`facility_code`),
            `facility_name` = VALUES(`facility_name`),
            `exam_date` = VALUES(`exam_date`),
            `name_full_raw` = VALUES(`name_full_raw`),
            `name_kana_raw` = VALUES(`name_kana_raw`),
            `name_kana_match` = VALUES(`name_kana_match`),
            `insurance_symbol_raw` = VALUES(`insurance_symbol_raw`),
            `insurance_symbol_match` = VALUES(`insurance_symbol_match`),
            `insurance_number_raw` = VALUES(`insurance_number_raw`),
            `insurance_number_match` = VALUES(`insurance_number_match`),
            `insurance_branch_number_raw` = VALUES(`insurance_branch_number_raw`),
            `insurance_branch_number_match` = VALUES(`insurance_branch_number_match`),
            `birthdate` = VALUES(`birthdate`),
            `gender_code` = VALUES(`gender_code`),
            `gender_raw` = VALUES(`gender_raw`),
            `health_exam_report_category` = VALUES(`health_exam_report_category`),
            `program_code` = VALUES(`program_code`),
            `postal_code` = VALUES(`postal_code`),
            `address` = VALUES(`address`),
            `exam_facility_postal_code` = VALUES(`exam_facility_postal_code`),
            `exam_facility_address` = VALUES(`exam_facility_address`),
            `exam_facility_phone_number` = VALUES(`exam_facility_phone_number`),
            `exam_item_status` = VALUES(`exam_item_status`),
            `exam_item_count` = VALUES(`exam_item_count`),
            `exam_item_error_count` = VALUES(`exam_item_error_count`),
            `exam_item_reason` = VALUES(`exam_item_reason`),
            `row_status` = VALUES(`row_status`),
            `row_reason` = VALUES(`row_reason`),
            `check_status` = VALUES(`check_status`),
            `check_reason` = VALUES(`check_reason`),
            `xml_export_status` = CASE
                WHEN {qname(config.health_db)}.`exam_ledgers`.`xml_export_status` = 'EXPORTED'
                    THEN {qname(config.health_db)}.`exam_ledgers`.`xml_export_status`
                WHEN VALUES(`xml_export_status`) = 'EXPORTED' THEN VALUES(`xml_export_status`)
                ELSE VALUES(`xml_export_status`)
            END,
            `manual_export_approved` = VALUES(`manual_export_approved`),
            `manual_export_reason` = VALUES(`manual_export_reason`),
            `manual_export_approved_at` = VALUES(`manual_export_approved_at`),
            `manual_export_approved_by` = VALUES(`manual_export_approved_by`),
            `resume_approved` = VALUES(`resume_approved`),
            `resume_approved_at` = VALUES(`resume_approved_at`),
            `resume_approved_by` = VALUES(`resume_approved_by`),
            `resume_approved_reason` = VALUES(`resume_approved_reason`),
            `source_updated_at` = VALUES(`source_updated_at`)
        """,
        (config.event_id,),
    )
    return int(cur.rowcount)


def upsert_source_rows(cur: Any, config: SyncConfig, source_type: str) -> int:
    if source_type == "XML":
        source_id_column = "source_xml_ledger_id"
        file_expr = "el.`file_receipt_id`"
    elif source_type == "CSV":
        source_id_column = "source_csv_row_ledger_id"
        file_expr = "el.`file_receipt_id`"
    else:
        raise ValueError(f"unsupported source_type: {source_type}")

    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.`exam_ledger_sources` (
            `exam_ledger_id`, `source_type`, `source_ledger_id`, `file_receipt_id`,
            `source_priority`, `source_role`, `source_status`, `source_reason`
        )
        SELECT
            el.`exam_ledger_id`,
            %s,
            el.{qname(source_id_column)},
            {file_expr},
            CASE WHEN %s = 'XML' THEN 10 ELSE 100 END,
            'PRIMARY',
            'ACTIVE',
            NULL
        FROM {qname(config.health_db)}.`exam_ledgers` AS el
        WHERE el.`event_id` = %s
          AND el.`source_type` = %s
          AND el.{qname(source_id_column)} IS NOT NULL
        ON DUPLICATE KEY UPDATE
            `exam_ledger_id` = VALUES(`exam_ledger_id`),
            `file_receipt_id` = VALUES(`file_receipt_id`),
            `source_priority` = VALUES(`source_priority`),
            `source_role` = VALUES(`source_role`),
            `source_status` = VALUES(`source_status`),
            `source_reason` = VALUES(`source_reason`)
        """,
        (source_type, source_type, config.event_id, source_type),
    )
    return int(cur.rowcount)


def sync_exam_ledgers(conn: Any, config: SyncConfig) -> SyncSummary:
    validate_config(config)
    cur = dict_cursor(conn)
    try:
        summary = load_summary(cur, config)
        if summary.source_rows == 0:
            raise RuntimeError(f"event_id={config.event_id} has no XML or CSV ledger rows")
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
            summary.xml_upserted_rows = upsert_xml_ledgers(cur, config)
            summary.csv_upserted_rows = upsert_csv_ledgers(cur, config)
            summary.xml_source_upserted_rows = upsert_source_rows(cur, config, "XML")
            summary.csv_source_upserted_rows = upsert_source_rows(cur, config, "CSV")
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.source_rows, rows_inserted=summary.changed_rows),
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
    config = SyncConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dry_run=bool(args.dry_run),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = sync_exam_ledgers(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
