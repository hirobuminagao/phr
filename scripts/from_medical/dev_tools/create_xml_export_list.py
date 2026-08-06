#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.from_medical.script_lib.hia_xml_export_loader import ExportSelectors, decide_candidate, fetch_candidates
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.examination.lookup import qname


@dataclass(frozen=True)
class Config:
    event_id: int
    list_name: str
    health_db: str
    master_db: str
    facility_codes: tuple[str, ...]
    all_facilities: bool
    facility_ids: tuple[int, ...]
    file_receipt_ids: tuple[int, ...]
    case_ids: tuple[int, ...]
    subscriber_ids: tuple[int, ...]
    hia_subscriber_ids: tuple[str, ...]
    person_id_customs: tuple[str, ...]
    exam_month: str | None
    include_exported: bool
    requested_file_date: date | None
    requested_split_no: int | None
    created_by: str | None
    confirm: bool
    dry_run: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create an XML export list from exam export cases.")
    parser.add_argument("--event-id", type=int, default=2)
    parser.add_argument("--list-name", required=True)
    parser.add_argument("--facility-code", action="append", default=[])
    parser.add_argument("--all-facilities", action="store_true")
    parser.add_argument("--facility-id", type=int, action="append", default=[])
    parser.add_argument("--file-receipt-id", type=int, action="append", default=[])
    parser.add_argument("--case-id", type=int, action="append", default=[])
    parser.add_argument("--subscriber-id", type=int, action="append", default=[])
    parser.add_argument("--hia-subscriber-id", action="append", default=[])
    parser.add_argument("--person-id-custom", action="append", default=[])
    parser.add_argument("--exam-month", help="YYYY-MM")
    parser.add_argument("--include-exported", action="store_true")
    parser.add_argument("--file-date", help="YYYYMMDD; stored as requested file_date for export")
    parser.add_argument("--split-no", type=int, choices=range(10))
    parser.add_argument("--created-by")
    parser.add_argument("--confirm", action="store_true", help="Create the list as READY instead of DRAFT.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default="health_exam_result")
    parser.add_argument("--master-db", default="phr_master")
    return parser.parse_args()


def parse_file_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y%m%d").date()


def load_config(args: argparse.Namespace) -> Config:
    return Config(
        event_id=args.event_id,
        list_name=args.list_name,
        health_db=args.health_db,
        master_db=args.master_db,
        facility_codes=tuple(args.facility_code or ()),
        all_facilities=bool(args.all_facilities),
        facility_ids=tuple(args.facility_id or ()),
        file_receipt_ids=tuple(args.file_receipt_id or ()),
        case_ids=tuple(args.case_id or ()),
        subscriber_ids=tuple(args.subscriber_id or ()),
        hia_subscriber_ids=tuple(args.hia_subscriber_id or ()),
        person_id_customs=tuple(args.person_id_custom or ()),
        exam_month=args.exam_month,
        include_exported=bool(args.include_exported),
        requested_file_date=parse_file_date(args.file_date),
        requested_split_no=args.split_no,
        created_by=args.created_by,
        confirm=bool(args.confirm),
        dry_run=bool(args.dry_run),
    )


def selector_summary(config: Config) -> str:
    lines = [
        f"event_id={config.event_id}",
        f"exam_month={config.exam_month or ''}",
        f"all_facilities={int(config.all_facilities)}",
        f"facility_codes={','.join(config.facility_codes)}",
        f"facility_ids={','.join(str(item) for item in config.facility_ids)}",
        f"file_receipt_ids={','.join(str(item) for item in config.file_receipt_ids)}",
        f"case_ids={','.join(str(item) for item in config.case_ids)}",
        f"subscriber_ids={','.join(str(item) for item in config.subscriber_ids)}",
        f"hia_subscriber_ids={','.join(config.hia_subscriber_ids)}",
        f"person_id_customs={','.join(config.person_id_customs)}",
        f"include_exported={int(config.include_exported)}",
    ]
    return "\n".join(lines)


def create_list(cur: Any, config: Config, rows: list[dict[str, Any]]) -> int:
    status = "READY" if config.confirm else "DRAFT"
    confirmed_by = config.created_by if config.confirm else None
    cur.execute(
        f"""
        INSERT INTO {qname(config.health_db)}.xml_export_lists (
          event_id, list_name, list_status, selector_summary,
          requested_exam_month, requested_facility_codes, include_exported,
          requested_file_date, requested_split_no, created_by, confirmed_by, confirmed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE CURRENT_TIMESTAMP(3) END)
        """,
        (
            config.event_id,
            config.list_name,
            status,
            selector_summary(config),
            config.exam_month,
            "\n".join(config.facility_codes) if config.facility_codes else None,
            config.include_exported,
            config.requested_file_date,
            config.requested_split_no,
            config.created_by,
            confirmed_by,
            confirmed_by,
        ),
    )
    list_id = int(cur.lastrowid)
    for row in rows:
        cur.execute(
            f"""
            INSERT INTO {qname(config.health_db)}.xml_export_list_cases (
              xml_export_list_id, exam_export_case_id, list_case_status,
              export_readiness_status_snapshot, export_readiness_reason_snapshot,
              added_by
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                list_id,
                row["exam_export_case_id"],
                "READY" if config.confirm else "SELECTED",
                row.get("export_readiness_status"),
                row.get("export_readiness_reason"),
                config.created_by,
            ),
        )
    return list_id


def run(config: Config, *, db_prefix: str) -> int:
    if (
        not config.all_facilities
        and not config.facility_codes
        and not config.facility_ids
        and not config.file_receipt_ids
        and not config.case_ids
        and not config.subscriber_ids
        and not config.hia_subscriber_ids
        and not config.person_id_customs
    ):
        raise ValueError("Specify --all-facilities or at least one selector.")
    selectors = ExportSelectors(
        event_id=config.event_id,
        facility_ids=config.facility_ids,
        facility_codes=config.facility_codes,
        file_receipt_ids=config.file_receipt_ids,
        ledger_ids=config.case_ids,
        subscriber_ids=config.subscriber_ids,
        hia_subscriber_ids=config.hia_subscriber_ids,
        person_id_customs=config.person_id_customs,
        exam_month=config.exam_month,
        include_exported=config.include_exported,
    )
    params = load_mysql_base_params(db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        with dict_cursor(conn) as cur:
            candidates = fetch_candidates(cur, selectors=selectors, health_db=config.health_db, master_db=config.master_db)
            rows = [row for row in candidates if decide_candidate(row).allowed]
            if config.dry_run:
                print(
                    f"xml_export_list dry_run=1 candidates={len(candidates)} selected={len(rows)} "
                    f"list_name={config.list_name}"
                )
                conn.rollback()
                return 0
            list_id = create_list(cur, config, rows)
            conn.commit()
            print(f"xml_export_list_id={list_id} candidates={len(candidates)} selected={len(rows)} status={'READY' if config.confirm else 'DRAFT'}")
            return 0


def main() -> int:
    args = parse_args()
    try:
        return run(load_config(args), db_prefix=args.db_prefix)
    except Exception as exc:
        print(f"CREATE_XML_EXPORT_LIST_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
