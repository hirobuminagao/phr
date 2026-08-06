#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.examination.lookup import qname


ZIP_STATUSES = {"PENDING", "UPLOADED", "UPLOAD_ERROR", "PARTIAL", "CONFIRMED"}
MEMBER_STATUSES = {"PENDING", "UPLOADED", "UPLOAD_ERROR", "EXCLUDED"}


@dataclass(frozen=True)
class Config:
    health_db: str
    zip_id: int | None
    member_ids: tuple[int, ...]
    zip_status: str | None
    member_status: str | None
    by: str | None
    note: str | None
    error_code: str | None
    error_message: str | None
    apply_to_members: bool
    dry_run: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update HIA upload status for exported XML ZIP/member history.")
    parser.add_argument("--zip-id", type=int, help="health_exam_result.xml_export_zips.xml_export_zip_id")
    parser.add_argument("--member-id", type=int, action="append", default=[], help="health_exam_result.xml_export_members.xml_export_member_id")
    parser.add_argument("--zip-status", choices=sorted(ZIP_STATUSES))
    parser.add_argument("--member-status", choices=sorted(MEMBER_STATUSES))
    parser.add_argument("--by", help="Operator name")
    parser.add_argument("--note")
    parser.add_argument("--error-code")
    parser.add_argument("--error-message")
    parser.add_argument("--apply-to-members", action="store_true", help="When --zip-id is specified, update all members in the ZIP too.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default="health_exam_result")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> Config:
    member_ids = tuple(args.member_id or ())
    if args.zip_id is None and not member_ids:
        raise ValueError("Specify --zip-id or --member-id.")
    if args.zip_id is not None and args.zip_status is None and not args.apply_to_members:
        raise ValueError("Specify --zip-status, or use --apply-to-members with --member-status.")
    if member_ids and args.member_status is None:
        raise ValueError("--member-status is required when --member-id is specified.")
    if args.apply_to_members and args.member_status is None:
        raise ValueError("--member-status is required when --apply-to-members is specified.")
    if args.member_status != "UPLOAD_ERROR" and (args.error_code or args.error_message):
        raise ValueError("--error-code/--error-message require --member-status UPLOAD_ERROR.")
    if args.zip_status != "UPLOAD_ERROR" and args.error_message and not args.member_status:
        raise ValueError("--error-message for ZIP requires --zip-status UPLOAD_ERROR.")
    return Config(
        health_db=args.health_db,
        zip_id=args.zip_id,
        member_ids=member_ids,
        zip_status=args.zip_status,
        member_status=args.member_status,
        by=args.by,
        note=args.note,
        error_code=args.error_code,
        error_message=args.error_message,
        apply_to_members=bool(args.apply_to_members),
        dry_run=bool(args.dry_run),
    )


def update_zip(cur: Any, config: Config) -> int:
    if config.zip_id is None or config.zip_status is None:
        return 0
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.xml_export_zips
        SET
          hia_upload_status = %s,
          hia_uploaded_at = CASE WHEN %s IN ('UPLOADED', 'CONFIRMED') THEN COALESCE(hia_uploaded_at, CURRENT_TIMESTAMP(3)) ELSE hia_uploaded_at END,
          hia_uploaded_by = CASE WHEN %s IN ('UPLOADED', 'CONFIRMED') THEN COALESCE(%s, hia_uploaded_by) ELSE hia_uploaded_by END,
          hia_upload_checked_at = CASE WHEN %s IN ('UPLOADED', 'UPLOAD_ERROR', 'PARTIAL', 'CONFIRMED') THEN CURRENT_TIMESTAMP(3) ELSE hia_upload_checked_at END,
          hia_upload_checked_by = CASE WHEN %s IN ('UPLOADED', 'UPLOAD_ERROR', 'PARTIAL', 'CONFIRMED') THEN COALESCE(%s, hia_upload_checked_by) ELSE hia_upload_checked_by END,
          hia_upload_error_summary = CASE WHEN %s = 'UPLOAD_ERROR' THEN %s ELSE hia_upload_error_summary END,
          hia_upload_note = COALESCE(%s, hia_upload_note)
        WHERE xml_export_zip_id = %s
        """,
        (
            config.zip_status,
            config.zip_status,
            config.zip_status,
            config.by,
            config.zip_status,
            config.zip_status,
            config.by,
            config.zip_status,
            config.error_message,
            config.note,
            config.zip_id,
        ),
    )
    return int(cur.rowcount)


def update_members_by_zip(cur: Any, config: Config) -> int:
    if config.zip_id is None or not config.apply_to_members or config.member_status is None:
        return 0
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.xml_export_members
        SET
          hia_upload_status = %s,
          hia_upload_error_code = CASE WHEN %s = 'UPLOAD_ERROR' THEN %s ELSE hia_upload_error_code END,
          hia_upload_error_message = CASE WHEN %s = 'UPLOAD_ERROR' THEN %s ELSE hia_upload_error_message END,
          hia_upload_note = COALESCE(%s, hia_upload_note),
          hia_uploaded_at = CASE WHEN %s = 'UPLOADED' THEN COALESCE(hia_uploaded_at, CURRENT_TIMESTAMP(3)) ELSE hia_uploaded_at END,
          hia_uploaded_by = CASE WHEN %s = 'UPLOADED' THEN COALESCE(%s, hia_uploaded_by) ELSE hia_uploaded_by END
        WHERE xml_export_zip_id = %s
        """,
        (
            config.member_status,
            config.member_status,
            config.error_code,
            config.member_status,
            config.error_message,
            config.note,
            config.member_status,
            config.member_status,
            config.by,
            config.zip_id,
        ),
    )
    return int(cur.rowcount)


def update_members_by_id(cur: Any, config: Config) -> int:
    if not config.member_ids or config.member_status is None:
        return 0
    placeholders = ", ".join(["%s"] * len(config.member_ids))
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.xml_export_members
        SET
          hia_upload_status = %s,
          hia_upload_error_code = CASE WHEN %s = 'UPLOAD_ERROR' THEN %s ELSE hia_upload_error_code END,
          hia_upload_error_message = CASE WHEN %s = 'UPLOAD_ERROR' THEN %s ELSE hia_upload_error_message END,
          hia_upload_note = COALESCE(%s, hia_upload_note),
          hia_uploaded_at = CASE WHEN %s = 'UPLOADED' THEN COALESCE(hia_uploaded_at, CURRENT_TIMESTAMP(3)) ELSE hia_uploaded_at END,
          hia_uploaded_by = CASE WHEN %s = 'UPLOADED' THEN COALESCE(%s, hia_uploaded_by) ELSE hia_uploaded_by END
        WHERE xml_export_member_id IN ({placeholders})
        """,
        (
            config.member_status,
            config.member_status,
            config.error_code,
            config.member_status,
            config.error_message,
            config.note,
            config.member_status,
            config.member_status,
            config.by,
            *config.member_ids,
        ),
    )
    return int(cur.rowcount)


def run(config: Config, *, db_prefix: str) -> int:
    params = load_mysql_base_params(db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        with dict_cursor(conn) as cur:
            zip_rows = update_zip(cur, config)
            member_rows = update_members_by_zip(cur, config) + update_members_by_id(cur, config)
            if config.dry_run:
                conn.rollback()
            else:
                conn.commit()
            print(
                f"update_hia_xml_upload_status dry_run={int(config.dry_run)} "
                f"zip_rows={zip_rows} member_rows={member_rows}"
            )
            return 0


def main() -> int:
    args = parse_args()
    try:
        return run(load_config(args), db_prefix=args.db_prefix)
    except Exception as exc:
        print(f"UPDATE_HIA_XML_UPLOAD_STATUS_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
