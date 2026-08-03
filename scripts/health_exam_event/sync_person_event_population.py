#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync event insurer subscriber population into dev_phr.person_event."""

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


DEV_DB = "dev_phr"
ETL_PHASE = "apply"
ETL_SOURCE = "FROM_MEDICAL"
ETL_DETAIL = "SYNC_PERSON_EVENT_POPULATION"


@dataclass(frozen=True)
class SyncConfig:
    event_id: int
    dev_db: str
    dry_run: bool


@dataclass
class SyncSummary:
    event_id: int
    dry_run: bool
    insurer_number: str | None = None
    subscriber_rows: int = 0
    existing_person_events: int = 0
    upserted_person_events: int = 0
    deleted_status_items: int = 0
    inserted_status_items: int = 0

    @property
    def changed_rows(self) -> int:
        return self.upserted_person_events + self.deleted_status_items + self.inserted_status_items

    def to_message(self) -> str:
        return (
            f"sync_person_event_population event_id={self.event_id} "
            f"insurer_number={self.insurer_number or ''} "
            f"subscribers={self.subscriber_rows} "
            f"existing_person_events={self.existing_person_events} "
            f"upsert_person_events={self.upserted_person_events} "
            f"delete_items={self.deleted_status_items} insert_items={self.inserted_status_items} "
            f"dry_run={self.dry_run}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync event insurer subscriber population into dev_phr.person_event."
    )
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--dev-db", default=DEV_DB)
    return parser.parse_args()


def qname(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f"`{name}`"


def validate_config(config: SyncConfig) -> None:
    if config.event_id <= 0:
        raise ValueError("event_id must be positive")
    qname(config.dev_db)


def scalar(cur: Any, sql: str, params: tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    row = cur.fetchone() or {}
    return int(row.get("cnt") or 0)


def get_event_insurer_number(cur: Any, config: SyncConfig) -> str:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        SELECT `insurer_number`
        FROM {dev}.`event`
        WHERE `event_id` = %s
          AND `is_active` = 1
        LIMIT 1
        """,
        (config.event_id,),
    )
    row = cur.fetchone() or {}
    insurer_number = str(row.get("insurer_number") or "").strip()
    if not insurer_number:
        raise RuntimeError(f"active event insurer_number not found: event_id={config.event_id}")
    return insurer_number.zfill(8)


def load_summary(cur: Any, config: SyncConfig) -> SyncSummary:
    dev = qname(config.dev_db)
    insurer_number = get_event_insurer_number(cur, config)
    return SyncSummary(
        event_id=config.event_id,
        dry_run=config.dry_run,
        insurer_number=insurer_number,
        subscriber_rows=scalar(
            cur,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {dev}.`subscribers`
            WHERE `insurer_number` = %s
            """,
            (insurer_number,),
        ),
        existing_person_events=scalar(
            cur,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {dev}.`person_event`
            WHERE `event_id` = %s
            """,
            (config.event_id,),
        ),
    )


def create_temp_population(cur: Any, config: SyncConfig, insurer_number: str) -> None:
    dev = qname(config.dev_db)
    cur.execute("DROP TEMPORARY TABLE IF EXISTS `tmp_person_event_population`")
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE `tmp_person_event_population` AS
        SELECT
            %s AS `event_id`,
            s.`id` AS `subscriber_id`,
            s.`person_id_custom`,
            s.`identity_hash`,
            s.`qualification_lost_date`,
            CASE
              WHEN s.`qualification_lost_date` IS NULL THEN 'ACTIVE_OR_NOT_LOST'
              ELSE 'QUALIFICATION_LOST'
            END AS `qualification_status`,
            s.`updated_at` AS `last_observed_at`
        FROM {dev}.`subscribers` AS s
        WHERE s.`insurer_number` = %s
        """,
        (config.event_id, insurer_number),
    )
    cur.execute("ALTER TABLE `tmp_person_event_population` ADD PRIMARY KEY (`event_id`, `subscriber_id`)")


def upsert_person_events(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        INSERT INTO {dev}.`person_event` (
            `event_id`, `subscriber_id`, `person_id_custom`, `identity_hash`,
            `is_eligible`, `last_observed_at`
        )
        SELECT
            t.`event_id`, t.`subscriber_id`,
            COALESCE(t.`person_id_custom`, ''),
            COALESCE(t.`identity_hash`, ''),
            1,
            t.`last_observed_at`
        FROM `tmp_person_event_population` AS t
        ON DUPLICATE KEY UPDATE
            `person_id_custom` = VALUES(`person_id_custom`),
            `identity_hash` = VALUES(`identity_hash`),
            `is_eligible` = VALUES(`is_eligible`),
            `last_observed_at` = GREATEST(
                COALESCE(`last_observed_at`, TIMESTAMP '1970-01-01 00:00:00'),
                COALESCE(VALUES(`last_observed_at`), TIMESTAMP '1970-01-01 00:00:00')
            )
        """
    )
    return int(cur.rowcount)


def delete_population_status_items(cur: Any, config: SyncConfig) -> int:
    dev = qname(config.dev_db)
    cur.execute(
        f"""
        DELETE i
        FROM {dev}.`person_event_status_items` AS i
        JOIN {dev}.`person_event` AS p
          ON p.`person_event_id` = i.`person_event_id`
        WHERE p.`event_id` = %s
          AND i.`source_system` = %s
          AND i.`item_code` IN (
            'EVENT_POPULATION_STATUS',
            'QUALIFICATION_LOST_DATE',
            'QUALIFICATION_STATUS'
          )
        """,
        (config.event_id, ETL_SOURCE),
    )
    return int(cur.rowcount)


def insert_population_status_items(cur: Any, config: SyncConfig, run_id: int) -> int:
    dev = qname(config.dev_db)

    def run_insert(value_sql: str) -> int:
        cur.execute(
            f"""
            INSERT INTO {dev}.`person_event_status_items` (
                `person_event_id`, `event_id`, `subscriber_id`, `item_code`, `value_type`,
                `value_bool`, `value_number`, `value_text`, `value_code`,
                `value_date`, `value_ref_type`, `value_ref_id`,
                `reason`, `source_system`, `source_run_id`, `refreshed_at`
            )
            SELECT
                p.`person_event_id`, t.`event_id`, t.`subscriber_id`,
                {value_sql},
                %s, %s, NOW()
            FROM `tmp_person_event_population` AS t
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
        'EVENT_POPULATION_STATUS', 'CODE',
        NULL, NULL, NULL, 'IN_EVENT_INSURER_POPULATION',
        NULL, NULL, NULL,
        'event insurer_number subscriber population'
        """
    )
    total += run_insert(
        """
        'QUALIFICATION_STATUS', 'CODE',
        NULL, NULL, NULL, t.`qualification_status`,
        NULL, NULL, NULL,
        CASE
          WHEN t.`qualification_lost_date` IS NULL THEN NULL
          ELSE CONCAT('qualification_lost_date=', DATE_FORMAT(t.`qualification_lost_date`, '%Y-%m-%d'))
        END
        """
    )
    total += run_insert(
        """
        'QUALIFICATION_LOST_DATE', 'DATE',
        NULL, NULL, NULL, NULL,
        t.`qualification_lost_date`, NULL, NULL,
        NULL
        """
    )
    return total


def sync_person_event_population(conn: Any, config: SyncConfig) -> SyncSummary:
    validate_config(config)
    cur = dict_cursor(conn)
    try:
        summary = load_summary(cur, config)
        if summary.subscriber_rows == 0:
            raise RuntimeError(
                f"event_id={config.event_id} insurer_number={summary.insurer_number} has no subscribers"
            )
        if config.dry_run:
            return summary

        run_id = etl_start_run(
            cur,
            phase=ETL_PHASE,
            source=ETL_SOURCE,
            db_schema=config.dev_db,
            db_path=config.dev_db,
            input_base=f"event_id={config.event_id}",
            input_file=None,
            insurer_number=summary.insurer_number,
            dry_run=False,
            limit_rows=None,
        )
        conn.commit()

        try:
            create_temp_population(cur, config, summary.insurer_number or "")
            summary.upserted_person_events = upsert_person_events(cur, config)
            summary.deleted_status_items = delete_population_status_items(cur, config)
            summary.inserted_status_items = insert_population_status_items(cur, config, run_id)
            etl_finish_run(
                cur,
                run_id,
                RunMetrics(rows_seen=summary.subscriber_rows, rows_inserted=summary.changed_rows),
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
                RunMetrics(rows_seen=summary.subscriber_rows, errors=1),
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
        dev_db=args.dev_db,
        dry_run=bool(args.dry_run),
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.dev_db, autocommit=False) as conn:
        summary = sync_person_event_population(conn, config)
    print(summary.to_message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
