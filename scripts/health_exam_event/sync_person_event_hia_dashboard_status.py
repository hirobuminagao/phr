#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync HIA dashboard current status into dev_phr.person_event status items."""

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


WORK_DB = "work_other"
DEV_DB = "dev_phr"
ETL_PHASE = "apply"
ETL_SOURCE = "HIA_DASHBOARD"
ETL_DETAIL = "SYNC_PERSON_EVENT_HIA_DASHBOARD_STATUS"

HIA_STATUS_ITEM_CODES = (
    "HIA_DASHBOARD_ACTIVE",
    "HIA_DASHBOARD_STATUS",
    "HIA_RESERVATION_DATE",
    "HIA_EXAM_DATE",
    "HIA_MEDICAL_INSTITUTION",
    "HIA_COURSE_NAME",
    "HIA_COMPANY_NAME",
    "HIA_DEPARTMENT_NAME",
    "HIA_EXCLUSION_REASON",
    "HIA_LAST_SEEN_RUN_ID",
    "HIA_INACTIVE_AT",
    "HIA_INACTIVE_REASON",
)


@dataclass(frozen=True)
class SyncConfig:
    event_id: int
    work_db: str
    dev_db: str
    dry_run: bool


@dataclass
class SyncSummary:
    event_id: int
    dry_run: bool
    insurer_number: str | None = None
    dashboard_rows: int = 0
    matched_people: int = 0
    deleted_status_items: int = 0
    inserted_status_items: int = 0
    updated_person_events: int = 0

    def to_message(self) -> str:
        return (
            f"sync_person_event_hia_dashboard_status event_id={self.event_id} "
            f"insurer_number={self.insurer_number or ''} dashboard_rows={self.dashboard_rows} "
            f"matched_people={self.matched_people} update_person_events={self.updated_person_events} "
            f"delete_items={self.deleted_status_items} insert_items={self.inserted_status_items} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync work_other.hia_dashboard_status into dev_phr.person_event_status_items."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--work-db", default=WORK_DB)
    parser.add_argument("--dev-db", default=DEV_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def scalar(cur: Any, sql: str, params: tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def validate_config(config: SyncConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    qname(config.work_db)
    qname(config.dev_db)


def get_event_insurer_number(cur: Any, config: SyncConfig) -> str:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        SELECT insurer_number
        FROM {dev}.event
        WHERE event_id = %s
          AND is_active = 1
        LIMIT 1
        """,
        (config.event_id,),
    )
    row = cur.fetchone() or {}
    insurer_number = str(row.get("insurer_number") or "").strip()
    if not insurer_number:
        raise RuntimeError(f"active event insurer_number not found: event_id={config.event_id}")
    return insurer_number.zfill(8)


def create_temp_dashboard_status(cur: Any, config: SyncConfig, insurer_number: str) -> None:
    work = qname(config.work_db)
    dev = qname(config.dev_db)
    cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_person_event_hia_dashboard_status")
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE tmp_person_event_hia_dashboard_status AS
        SELECT
          p.person_event_id,
          p.event_id,
          p.subscriber_id,
          d.hia_dashboard_person_id,
          d.hia_subscriber_id,
          d.identity_hash,
          d.status,
          d.reservation_date,
          d.exam_date,
          d.medical_institution,
          d.course_name,
          d.company_name,
          d.department_name,
          d.exclusion_reason,
          d.is_active,
          d.last_seen_run_id,
          d.inactive_at,
          d.inactive_reason,
          d.updated_at
        FROM {work}.hia_dashboard_status AS d
        JOIN {dev}.person_event AS p
          ON p.event_id = %s
         AND p.subscriber_id = d.subscribers_id
        WHERE d.insurer_number = %s
        """,
        (config.event_id, insurer_number),
    )
    cur.execute("ALTER TABLE tmp_person_event_hia_dashboard_status ADD PRIMARY KEY (person_event_id)")


def load_summary(cur: Any, config: SyncConfig, insurer_number: str) -> SyncSummary:
    work = qname(config.work_db)
    return SyncSummary(
        event_id=config.event_id,
        dry_run=config.dry_run,
        insurer_number=insurer_number,
        dashboard_rows=scalar(
            cur,
            f"SELECT COUNT(*) AS cnt FROM {work}.hia_dashboard_status WHERE insurer_number = %s",
            (insurer_number,),
        ),
        matched_people=scalar(
            cur,
            "SELECT COUNT(*) AS cnt FROM tmp_person_event_hia_dashboard_status",
            (),
        ),
    )


def delete_status_items(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    placeholders = ", ".join(["%s"] * len(HIA_STATUS_ITEM_CODES))
    cur.execute(
        f"""
        DELETE i
        FROM {dev}.person_event_status_items AS i
        JOIN {dev}.person_event AS p
          ON p.person_event_id = i.person_event_id
        WHERE p.event_id = %s
          AND i.source_system = %s
          AND i.item_code IN ({placeholders})
        """,
        (config.event_id, ETL_SOURCE, *HIA_STATUS_ITEM_CODES),
    )
    return int(cur.rowcount)


def insert_status_items(cur: Any, config: SyncConfig, run_id: int) -> int:
    dev = qname(config.dev_db)
    target = f"""
        INSERT INTO {dev}.person_event_status_items (
          person_event_id, event_id, subscriber_id, item_code, value_type,
          value_bool, value_number, value_text, value_code,
          value_date, value_datetime, value_ref_type, value_ref_id,
          reason, source_system, source_run_id, refreshed_at
        )
    """

    def run_insert(value_sql: str) -> int:
        cur.execute(
            target
            + f"""
            SELECT
              t.person_event_id, t.event_id, t.subscriber_id,
              {value_sql},
              %s, %s, NOW()
            FROM tmp_person_event_hia_dashboard_status AS t
            """,
            (ETL_SOURCE, run_id),
        )
        return int(cur.rowcount)

    total = 0
    total += run_insert(
        """
        'HIA_DASHBOARD_ACTIVE', 'BOOL',
        t.is_active, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        CASE WHEN t.is_active = 1 THEN NULL ELSE t.inactive_reason END
        """
    )
    code_items = [
        ("HIA_DASHBOARD_STATUS", "status"),
        ("HIA_INACTIVE_REASON", "inactive_reason"),
    ]
    for item_code, column in code_items:
        total += run_insert(
            f"""
            '{item_code}', 'CODE',
            NULL, NULL, NULLIF(t.{column}, ''),
            NULL, NULL, NULL, NULL, NULL
            """
        )
    date_items = [
        ("HIA_RESERVATION_DATE", "reservation_date"),
        ("HIA_EXAM_DATE", "exam_date"),
    ]
    for item_code, column in date_items:
        total += run_insert(
            f"""
            '{item_code}', 'DATE',
            NULL, NULL, NULL, NULL,
            t.{column}, NULL, NULL, NULL, NULL
            """
        )
    text_items = [
        ("HIA_MEDICAL_INSTITUTION", "medical_institution"),
        ("HIA_COURSE_NAME", "course_name"),
        ("HIA_COMPANY_NAME", "company_name"),
        ("HIA_DEPARTMENT_NAME", "department_name"),
        ("HIA_EXCLUSION_REASON", "exclusion_reason"),
    ]
    for item_code, column in text_items:
        total += run_insert(
            f"""
            '{item_code}', 'TEXT',
            NULL, NULL, NULLIF(t.{column}, ''), NULL,
            NULL, NULL, NULL, NULL, NULL
            """
        )
    total += run_insert(
        """
        'HIA_LAST_SEEN_RUN_ID', 'REF',
        NULL, NULL, NULL, NULL,
        NULL, NULL, 'ETL_RUN', t.last_seen_run_id, NULL
        """
    )
    total += run_insert(
        """
        'HIA_INACTIVE_AT', 'DATETIME',
        NULL, NULL, NULL, NULL,
        NULL, t.inactive_at, NULL, NULL, NULL
        """
    )
    return total


def update_person_events(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        UPDATE {dev}.person_event AS p
        JOIN tmp_person_event_hia_dashboard_status AS t
          ON t.person_event_id = p.person_event_id
        SET p.hia_status_code = t.status,
            p.last_observed_at = GREATEST(
              COALESCE(p.last_observed_at, TIMESTAMP '1970-01-01 00:00:00'),
              COALESCE(t.updated_at, TIMESTAMP '1970-01-01 00:00:00')
            )
        WHERE p.event_id = %s
        """,
        (config.event_id,),
    )
    return int(cur.rowcount)


def sync_person_event_hia_dashboard_status(conn: Any, config: SyncConfig) -> SyncSummary:
    validate_config(config)
    cur = dict_cursor(conn)
    try:
        insurer_number = get_event_insurer_number(cur, config)
        create_temp_dashboard_status(cur, config, insurer_number)
        summary = load_summary(cur, config, insurer_number)
        if config.dry_run:
            return summary

        run_id = etl_start_run(
            cur,
            phase=ETL_PHASE,
            source=ETL_SOURCE,
            db_schema=config.work_db,
            db_path=config.work_db,
            input_base=f"event_id={config.event_id}",
            input_file=None,
            insurer_number=insurer_number,
            dry_run=False,
            limit_rows=None,
        )
        conn.commit()

        try:
            summary.deleted_status_items = delete_status_items(cur, config)
            summary.inserted_status_items = insert_status_items(cur, config, run_id)
            summary.updated_person_events = update_person_events(cur, config)
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.dashboard_rows, rows_inserted=summary.inserted_status_items),
                status_override="success",
                extra_notes=f"{ETL_DETAIL} {summary.to_message()}",
            )
            conn.commit()
            return summary
        except Exception as exc:
            conn.rollback()
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.dashboard_rows, errors=1),
                status_override="failed",
                extra_notes=f"{ETL_DETAIL} {summary.to_message()} error={type(exc).__name__}: {exc}",
            )
            conn.commit()
            raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = SyncConfig(
        event_id=args.event_id,
        work_db=args.work_db,
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.work_db, autocommit=False) as conn:
        summary = sync_person_event_hia_dashboard_status(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
