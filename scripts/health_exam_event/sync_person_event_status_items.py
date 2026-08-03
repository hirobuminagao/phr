#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync exam_ledgers into dev_phr.person_event and vertical status items."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import start_run as etl_start_run


HEALTH_DB = "health_exam_result"
DEV_DB = "dev_phr"
ETL_PHASE = "SYNC_PERSON_EVENT_STATUS_ITEMS"
ETL_SOURCE = "FROM_MEDICAL"
RESULT_STATUS_ITEM_CODES = (
    "PERSON_STATUS",
    "RESULT_RECEIVED_COUNT",
    "MATCHED_LEDGER_COUNT",
    "CHECK_OK_LEDGER_COUNT",
    "CHECK_NG_LEDGER_COUNT",
    "CHECK_PENDING_LEDGER_COUNT",
    "EXPORTABLE_LEDGER_COUNT",
    "EXPORTED_LEDGER_COUNT",
    "LATEST_EXAM_LEDGER_ID",
    "LATEST_EXAM_DATE",
    "LATEST_FACILITY_CODE",
    "LATEST_FACILITY_NAME",
    "REQUIRES_BASIC_INFO_CORRECTION",
    "REQUIRES_MANUAL_EXPORT_APPROVAL",
)


@dataclass(frozen=True)
class SyncConfig:
    event_id: int
    health_db: str
    dev_db: str
    dry_run: bool


@dataclass
class SyncSummary:
    event_id: int
    dry_run: bool
    matched_ledgers: int = 0
    matched_people: int = 0
    existing_person_events: int = 0
    upserted_person_events: int = 0
    deleted_status_items: int = 0
    inserted_status_items: int = 0

    @property
    def changed_rows(self) -> int:
        return self.upserted_person_events + self.deleted_status_items + self.inserted_status_items

    def to_message(self) -> str:
        return (
            f"sync_person_event_status_items event_id={self.event_id} "
            f"matched_ledgers={self.matched_ledgers} matched_people={self.matched_people} "
            f"existing_person_events={self.existing_person_events} "
            f"upsert_person_events={self.upserted_person_events} "
            f"delete_items={self.deleted_status_items} insert_items={self.inserted_status_items} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync health_exam_result.exam_ledgers into dev_phr.person_event status items."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_DB)
    parser.add_argument("--dev-db", default=DEV_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: SyncConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    qname(config.health_db)
    qname(config.dev_db)


def scalar(cur: Any, sql: str, params: tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def create_temp_summary(cur: Any, config: SyncConfig) -> None:
    health = qname(config.health_db)
    dev = qname(config.dev_db)
    cur.execute("DROP TEMPORARY TABLE IF EXISTS `tmp_person_event_status_summary`")
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE `tmp_person_event_status_summary` AS
        SELECT
            a.`event_id`,
            a.`subscriber_id`,
            s.`person_id_custom`,
            s.`identity_hash`,
            COUNT(*) AS `result_received_count`,
            SUM(CASE WHEN a.`subscriber_match_status` = 'MATCHED' THEN 1 ELSE 0 END) AS `matched_ledger_count`,
            SUM(CASE WHEN a.`check_status` = 'OK' THEN 1 ELSE 0 END) AS `check_ok_ledger_count`,
            SUM(CASE WHEN a.`check_status` = 'NG' THEN 1 ELSE 0 END) AS `check_ng_ledger_count`,
            SUM(CASE WHEN a.`check_status` IS NULL OR a.`check_status` = 'PENDING' THEN 1 ELSE 0 END) AS `check_pending_ledger_count`,
            SUM(
              CASE
                WHEN a.`subscriber_match_status` = 'MATCHED'
                 AND a.`health_exam_report_category` IS NOT NULL
                 AND a.`program_code` IS NOT NULL
                 AND (a.`check_status` = 'OK' OR a.`manual_export_approved` = 1)
                THEN 1 ELSE 0
              END
            ) AS `exportable_ledger_count`,
            SUM(CASE WHEN a.`xml_export_status` = 'EXPORTED' THEN 1 ELSE 0 END) AS `exported_ledger_count`,
            MAX(CASE WHEN a.`correction_status` <> 'NONE' THEN 1 ELSE 0 END) AS `requires_basic_info_correction`,
            MAX(CASE WHEN a.`manual_export_approved` = 1 THEN 1 ELSE 0 END) AS `requires_manual_export_approval`,
            MAX(COALESCE(a.`source_updated_at`, a.`updated_at`)) AS `last_observed_at`,
            SUBSTRING_INDEX(
              GROUP_CONCAT(a.`exam_ledger_id` ORDER BY COALESCE(a.`source_updated_at`, a.`updated_at`) DESC, a.`exam_ledger_id` DESC),
              ',', 1
            ) AS `latest_exam_ledger_id`,
            SUBSTRING_INDEX(
              GROUP_CONCAT(COALESCE(a.`exam_date`, '') ORDER BY COALESCE(a.`source_updated_at`, a.`updated_at`) DESC, a.`exam_ledger_id` DESC),
              ',', 1
            ) AS `latest_exam_date`,
            SUBSTRING_INDEX(
              GROUP_CONCAT(COALESCE(a.`facility_code`, '') ORDER BY COALESCE(a.`source_updated_at`, a.`updated_at`) DESC, a.`exam_ledger_id` DESC),
              ',', 1
            ) AS `latest_facility_code`,
            SUBSTRING_INDEX(
              GROUP_CONCAT(COALESCE(a.`facility_name`, '') ORDER BY COALESCE(a.`source_updated_at`, a.`updated_at`) DESC, a.`exam_ledger_id` DESC),
              ',', 1
            ) AS `latest_facility_name`,
            SUBSTRING_INDEX(
              GROUP_CONCAT(COALESCE(a.`check_reason`, '') ORDER BY COALESCE(a.`source_updated_at`, a.`updated_at`) DESC, a.`exam_ledger_id` DESC SEPARATOR '\\n'),
              '\\n', 1
            ) AS `latest_check_reason`
        FROM {health}.`exam_ledgers` AS a
        JOIN {dev}.`subscribers` AS s
          ON s.`id` = a.`subscriber_id`
        WHERE a.`event_id` = %s
          AND a.`subscriber_id` IS NOT NULL
          AND a.`subscriber_match_status` = 'MATCHED'
        GROUP BY a.`event_id`, a.`subscriber_id`, s.`person_id_custom`, s.`identity_hash`
        """,
        (config.event_id,),
    )
    cur.execute("ALTER TABLE `tmp_person_event_status_summary` ADD PRIMARY KEY (`event_id`, `subscriber_id`)")


def load_summary(cur: Any, config: SyncConfig) -> SyncSummary:
    health = qname(config.health_db)
    dev = qname(config.dev_db)
    return SyncSummary(
        event_id=config.event_id,
        dry_run=config.dry_run,
        matched_ledgers=scalar(
            cur,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {health}.`exam_ledgers`
            WHERE `event_id` = %s
              AND `subscriber_id` IS NOT NULL
              AND `subscriber_match_status` = 'MATCHED'
            """,
            (config.event_id,),
        ),
        matched_people=scalar(
            cur,
            f"""
            SELECT COUNT(DISTINCT `subscriber_id`) AS cnt
            FROM {health}.`exam_ledgers`
            WHERE `event_id` = %s
              AND `subscriber_id` IS NOT NULL
              AND `subscriber_match_status` = 'MATCHED'
            """,
            (config.event_id,),
        ),
        existing_person_events=scalar(
            cur,
            f"SELECT COUNT(*) AS cnt FROM {dev}.`person_event` WHERE `event_id` = %s",
            (config.event_id,),
        ),
    )


def upsert_person_events(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        INSERT INTO {dev}.`person_event` (
            `event_id`, `subscriber_id`, `person_id_custom`, `identity_hash`,
            `result_received_count`, `last_result_received_at`,
            `is_eligible`, `result_received_flag`, `delivery_target_flag`,
            `delivery_exported_flag`, `gap_flag`, `gap_reason`, `last_observed_at`
        )
        SELECT
            t.`event_id`, t.`subscriber_id`, t.`person_id_custom`, t.`identity_hash`,
            t.`result_received_count`, t.`last_observed_at`,
            1, 1,
            CASE WHEN t.`exportable_ledger_count` > 0 THEN 1 ELSE 0 END,
            CASE WHEN t.`exported_ledger_count` > 0 THEN 1 ELSE 0 END,
            CASE WHEN t.`check_ng_ledger_count` > 0 OR t.`requires_basic_info_correction` = 1 THEN 1 ELSE 0 END,
            CASE
              WHEN t.`requires_basic_info_correction` = 1 THEN 'BASIC_INFO_CORRECTION_REQUIRED'
              WHEN t.`check_ng_ledger_count` > 0 THEN t.`latest_check_reason`
              ELSE NULL
            END,
            t.`last_observed_at`
        FROM `tmp_person_event_status_summary` AS t
        ON DUPLICATE KEY UPDATE
            `person_id_custom` = VALUES(`person_id_custom`),
            `identity_hash` = VALUES(`identity_hash`),
            `result_received_count` = VALUES(`result_received_count`),
            `last_result_received_at` = VALUES(`last_result_received_at`),
            `result_received_flag` = VALUES(`result_received_flag`),
            `delivery_target_flag` = VALUES(`delivery_target_flag`),
            `delivery_exported_flag` = VALUES(`delivery_exported_flag`),
            `gap_flag` = VALUES(`gap_flag`),
            `gap_reason` = VALUES(`gap_reason`),
            `last_observed_at` = VALUES(`last_observed_at`)
        """
    )
    return int(cur.rowcount)


def delete_status_items(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    placeholders = ", ".join(["%s"] * len(RESULT_STATUS_ITEM_CODES))
    cur.execute(
        f"""
        DELETE i
        FROM {dev}.`person_event_status_items` AS i
        JOIN {dev}.`person_event` AS p
          ON p.`person_event_id` = i.`person_event_id`
        WHERE p.`event_id` = %s
          AND i.`source_system` = %s
          AND i.`item_code` IN ({placeholders})
        """,
        (config.event_id, ETL_SOURCE, *RESULT_STATUS_ITEM_CODES),
    )
    return int(cur.rowcount)


def insert_status_items(cur: Any, config: SyncConfig, run_id: int) -> int:
    dev = qname(config.dev_db)
    target_columns = f"""
            INSERT INTO {dev}.`person_event_status_items` (
            `person_event_id`, `event_id`, `subscriber_id`, `item_code`, `value_type`,
            `value_bool`, `value_number`, `value_text`, `value_code`,
            `value_date`, `value_ref_type`, `value_ref_id`,
            `reason`, `source_system`, `source_run_id`, `refreshed_at`
        )
    """

    def run_insert(value_sql: str) -> int:
        cur.execute(
            target_columns
            + f"""
            SELECT
                p.`person_event_id`, t.`event_id`, t.`subscriber_id`,
                {value_sql},
                %s, %s, NOW()
            FROM `tmp_person_event_status_summary` AS t
            JOIN {dev}.`person_event` AS p
              ON p.`event_id` = t.`event_id`
             AND p.`subscriber_id` = t.`subscriber_id`
            """,
            (ETL_SOURCE, run_id),
        )
        return int(cur.rowcount)

    total = 0
    total += run_insert(
        """
        'PERSON_STATUS', 'CODE',
        NULL, NULL, NULL,
        CASE
          WHEN t.`exported_ledger_count` > 0 THEN 'XML_EXPORTED'
          WHEN t.`exportable_ledger_count` > 0 THEN 'XML_EXPORTABLE'
          WHEN t.`check_ng_ledger_count` > 0 THEN 'CHECK_NG'
          WHEN t.`check_pending_ledger_count` > 0 THEN 'CHECK_PENDING'
          ELSE 'RESULT_RECEIVED'
        END,
        NULL, NULL, NULL,
        t.`latest_check_reason`
        """
    )
    number_items = [
        ("RESULT_RECEIVED_COUNT", "result_received_count"),
        ("MATCHED_LEDGER_COUNT", "matched_ledger_count"),
        ("CHECK_OK_LEDGER_COUNT", "check_ok_ledger_count"),
        ("CHECK_NG_LEDGER_COUNT", "check_ng_ledger_count"),
        ("CHECK_PENDING_LEDGER_COUNT", "check_pending_ledger_count"),
        ("EXPORTABLE_LEDGER_COUNT", "exportable_ledger_count"),
        ("EXPORTED_LEDGER_COUNT", "exported_ledger_count"),
    ]
    for item_code, source_column in number_items:
        total += run_insert(
            f"""
            '{item_code}', 'NUMBER',
            NULL, t.`{source_column}`, NULL, NULL,
            NULL, NULL, NULL, NULL
            """
        )
    total += run_insert(
        """
        'LATEST_EXAM_LEDGER_ID', 'REF',
        NULL, NULL, NULL, NULL,
        NULL, 'EXAM_LEDGER', CAST(t.`latest_exam_ledger_id` AS UNSIGNED), NULL
        """
    )
    total += run_insert(
        """
        'LATEST_EXAM_DATE', 'DATE',
        NULL, NULL, NULL, NULL,
        NULLIF(t.`latest_exam_date`, ''), NULL, NULL, NULL
        """
    )
    text_items = [
        ("LATEST_FACILITY_CODE", "latest_facility_code"),
        ("LATEST_FACILITY_NAME", "latest_facility_name"),
    ]
    for item_code, source_column in text_items:
        total += run_insert(
            f"""
            '{item_code}', 'TEXT',
            NULL, NULL, NULLIF(t.`{source_column}`, ''), NULL,
            NULL, NULL, NULL, NULL
            """
        )
    bool_items = [
        ("REQUIRES_BASIC_INFO_CORRECTION", "requires_basic_info_correction"),
        ("REQUIRES_MANUAL_EXPORT_APPROVAL", "requires_manual_export_approval"),
    ]
    for item_code, source_column in bool_items:
        total += run_insert(
            f"""
            '{item_code}', 'BOOL',
            t.`{source_column}`, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL
            """
        )
    return total


def sync_person_event_status_items(conn: Any, config: SyncConfig) -> SyncSummary:
    validate_config(config)
    cur = dict_cursor(conn)
    try:
        summary = load_summary(cur, config)
        if summary.matched_ledgers == 0:
            raise RuntimeError(f"event_id={config.event_id} has no matched exam_ledger rows")
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
            create_temp_summary(cur, config)
            summary.upserted_person_events = upsert_person_events(cur, config)
            summary.deleted_status_items = delete_status_items(cur, config)
            summary.inserted_status_items = insert_status_items(cur, config, run_id)
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.matched_ledgers, rows_inserted=summary.inserted_status_items),
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
                RunMetrics(rows_seen=summary.matched_ledgers, errors=1),
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
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = sync_person_event_status_items(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
