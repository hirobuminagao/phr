#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Match scanned CSV file_receipts to registered CSV format versions."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.csv.exam_result_format_matcher import compact_text
from scripts.lib.csv.exam_result_format_matcher import match_csv_format_for_file
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import PHR_MASTER
from scripts.lib.etl import RunMetrics
from scripts.lib.etl import finish_run as etl_finish_run
from scripts.lib.etl import log_error as etl_log_error
from scripts.lib.etl import start_run as etl_start_run


HEALTH_EXAM_RESULT_DB = "health_exam_result"
DEV_PHR_DB = "dev_phr"
ETL_PHASE = "MATCH_CSV_FORMAT"
ETL_SOURCE = "FROM_MEDICAL"
FILE_STATUS_DISCOVERED = "DISCOVERED"
FILE_STATUS_READY = "READY"
FILE_STATUS_WAITING_CONFIRM = "WAITING_CONFIRM"


@dataclass(frozen=True)
class MatchConfig:
    event_id: int | None
    health_db: str
    dev_db: str
    master_db: str
    dry_run: bool
    limit: int
    include_ready: bool
    include_imported: bool


@dataclass
class MatchSummary:
    files: int = 0
    matched: int = 0
    not_found: int = 0
    multiple: int = 0
    skipped: int = 0
    errors: int = 0

    def to_metrics(self) -> RunMetrics:
        return RunMetrics(
            files=self.files,
            rows_seen=self.files,
            rows_inserted=0,
            rows_updated=self.matched + self.not_found + self.multiple,
            rows_skipped=self.skipped,
            errors=self.errors,
        )

    def to_message(self) -> str:
        return (
            f"match_csv_format files={self.files} matched={self.matched} "
            f"not_found={self.not_found} multiple={self.multiple} "
            f"skipped={self.skipped} errors={self.errors}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Match scanned CSV file_receipts to CSV format versions.")
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--include-ready", action="store_true")
    parser.add_argument("--include-imported", action="store_true")
    parser.add_argument("--db-prefix", default="PHR_DB_")
    parser.add_argument("--health-db", default=HEALTH_EXAM_RESULT_DB)
    parser.add_argument("--dev-db", default=DEV_PHR_DB)
    parser.add_argument("--master-db", default=PHR_MASTER)
    return parser.parse_args()


def qname(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def start_match_run(cur: Any, *, config: MatchConfig) -> int:
    return etl_start_run(
        cur,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        db_schema=config.health_db,
        db_path=config.health_db,
        input_base=f"event_id={config.event_id}" if config.event_id is not None else None,
        input_file=None,
        insurer_number=None,
        dry_run=config.dry_run,
        limit_rows=config.limit or None,
    )


def finish_match_run(cur: Any, *, run_id: int, summary: MatchSummary) -> None:
    status = "partial" if summary.errors or summary.not_found or summary.multiple else "success"
    etl_finish_run(
        cur,
        run_id,
        summary.to_metrics(),
        status_override=status,
        extra_notes=summary.to_message(),
    )


def record_match_error(
    cur: Any,
    *,
    run_id: int,
    src_file: str | None,
    field: str,
    error_code: str,
    message: str,
    field_value: str | None = None,
) -> None:
    etl_log_error(
        cur,
        run_id,
        phase=ETL_PHASE,
        source=ETL_SOURCE,
        insurer_number=None,
        src_file=src_file,
        row_no=None,
        line_no=None,
        field=field,
        field_value=field_value,
        error_code=error_code,
        message=message,
    )


def fetch_target_receipts(cur: Any, *, config: MatchConfig) -> list[dict[str, Any]]:
    statuses = [FILE_STATUS_DISCOVERED, FILE_STATUS_WAITING_CONFIRM]
    if config.include_ready:
        statuses.append(FILE_STATUS_READY)
    if config.include_imported:
        statuses.append("IMPORTED")

    params: list[Any] = list(statuses)
    where = ["file_type = 'CSV'", f"status IN ({', '.join(['%s'] * len(statuses))})"]
    if config.event_id is not None:
        where.append("event_id = %s")
        params.append(config.event_id)

    limit_sql = ""
    if config.limit:
        limit_sql = "LIMIT %s"
        params.append(config.limit)

    cur.execute(
        f"""
        SELECT *
        FROM {qname(config.health_db)}.file_receipts
        WHERE {" AND ".join(where)}
        ORDER BY id
        {limit_sql}
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def update_receipt_match(
    cur: Any,
    *,
    config: MatchConfig,
    receipt_id: int,
    actual_header_sha256: str | None,
    matched_csv_format_version_id: int | None,
    status: str,
    summary_message: str,
) -> None:
    cur.execute(
        f"""
        UPDATE {qname(config.health_db)}.file_receipts
        SET actual_header_sha256 = %s,
            matched_csv_format_version_id = %s,
            status = %s,
            summary_message = %s,
            content_checked_at = CURRENT_TIMESTAMP(3)
        WHERE id = %s
        """,
        (actual_header_sha256, matched_csv_format_version_id, status, summary_message, receipt_id),
    )


def match_receipt(cur: Any, *, config: MatchConfig, receipt: dict[str, Any]) -> tuple[str, str | None, int | None, str]:
    exam_facility_id = receipt.get("exam_facility_id")
    if exam_facility_id is None:
        return "ERROR", None, None, "CSV format match failed: exam_facility_id is not set."

    source_path = compact_text(receipt.get("source_path"))
    if source_path is None:
        return "ERROR", None, None, "CSV format match failed: source_path is not set."

    result = match_csv_format_for_file(
        cur,
        source_path=source_path,
        exam_facility_id=int(exam_facility_id),
        master_db=config.master_db,
    )
    return result.result, result.actual_header_sha256, result.csv_format_version_id, result.message


def run_match(conn: Any, config: MatchConfig) -> MatchSummary:
    summary = MatchSummary()
    run_id: int | None = None
    cur = dict_cursor(conn)
    try:
        if not config.dry_run:
            run_id = start_match_run(cur, config=config)
            conn.commit()

        receipts = fetch_target_receipts(cur, config=config)
        summary.files = len(receipts)

        for receipt in receipts:
            receipt_id = int(receipt["id"])
            src_file = compact_text(receipt.get("source_path"))
            try:
                result, actual_header_sha256, csv_format_version_id, message = match_receipt(
                    cur,
                    config=config,
                    receipt=receipt,
                )
            except Exception as exc:
                summary.errors += 1
                if not config.dry_run and run_id is not None:
                    update_receipt_match(
                        cur,
                        config=config,
                        receipt_id=receipt_id,
                        actual_header_sha256=None,
                        matched_csv_format_version_id=None,
                        status=FILE_STATUS_WAITING_CONFIRM,
                        summary_message=f"CSV format match failed: {type(exc).__name__}",
                    )
                    record_match_error(
                        cur,
                        run_id=run_id,
                        src_file=src_file,
                        field="CSV_FORMAT_MATCH",
                        error_code="CSV_FORMAT_MATCH_FAILED",
                        message=f"csv format match failed: file_receipt_id={receipt_id}: {exc}",
                        field_value=str(receipt_id),
                    )
                continue

            if result == "MATCHED":
                summary.matched += 1
                status = FILE_STATUS_READY
            elif result == "NOT_FOUND":
                summary.not_found += 1
                status = FILE_STATUS_WAITING_CONFIRM
            elif result == "MULTIPLE":
                summary.multiple += 1
                status = FILE_STATUS_WAITING_CONFIRM
            else:
                summary.errors += 1
                status = FILE_STATUS_WAITING_CONFIRM

            if not config.dry_run:
                update_receipt_match(
                    cur,
                    config=config,
                    receipt_id=receipt_id,
                    actual_header_sha256=actual_header_sha256,
                    matched_csv_format_version_id=csv_format_version_id,
                    status=status,
                    summary_message=message,
                )
                if result in {"NOT_FOUND", "MULTIPLE", "ERROR"} and run_id is not None:
                    record_match_error(
                        cur,
                        run_id=run_id,
                        src_file=src_file,
                        field="CSV_FORMAT_MATCH",
                        error_code=f"CSV_FORMAT_{result}",
                        message=message,
                        field_value=str(receipt_id),
                    )

        if not config.dry_run and run_id is not None:
            finish_match_run(cur, run_id=run_id, summary=summary)
            conn.commit()

        return summary
    except Exception as exc:
        conn.rollback()
        if not config.dry_run and run_id is not None:
            error_cur = dict_cursor(conn)
            try:
                record_match_error(
                    error_cur,
                    run_id=run_id,
                    src_file=None,
                    field="UNEXPECTED",
                    error_code="UNEXPECTED_CSV_FORMAT_MATCH_ERROR",
                    message=f"unexpected csv format match error: {type(exc).__name__}: {exc}",
                )
                etl_finish_run(
                    error_cur,
                    run_id,
                    summary.to_metrics(),
                    status_override="failed",
                    extra_notes=summary.to_message(),
                )
                conn.commit()
            finally:
                error_cur.close()
        raise
    finally:
        cur.close()


def main() -> int:
    args = parse_args()
    config = MatchConfig(
        event_id=args.event_id,
        health_db=args.health_db,
        dev_db=args.dev_db,
        master_db=args.master_db,
        dry_run=args.dry_run,
        limit=args.limit,
        include_ready=args.include_ready,
        include_imported=args.include_imported,
    )
    params = load_mysql_base_params(args.db_prefix)
    with connect_ctx(params, database=config.health_db, autocommit=False) as conn:
        summary = run_match(conn, config)
        if config.dry_run:
            conn.rollback()
        print(summary.to_message())
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
