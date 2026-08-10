#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.fund_delivery_list_builder import (  # noqa: E402
    FundDeliveryListConfig,
    build_fund_delivery_list,
)
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


DEFAULT_INSURER_NUMBER = "06139463"
DEFAULT_SENDER_CODE = "1322100106"


def default_list_name(exam_month: str | None, output_mode: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_mode == "EXAM_MONTH":
        return f"{exam_month}_健保納品リスト_{stamp}"
    return f"全件_健保納品リスト_{stamp}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a fund delivery output list from imported HIA XML ledgers.",
    )
    parser.add_argument("--database", default="health_exam_result")
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--insurer-number", default=DEFAULT_INSURER_NUMBER)
    parser.add_argument("--list-name", default=None)
    parser.add_argument("--output-mode", choices=("EXAM_MONTH", "ALL"), default="EXAM_MONTH")
    parser.add_argument("--exam-month", default=None, help="YYYYMM. Required when output-mode=EXAM_MONTH.")
    parser.add_argument(
        "--delivery-policy",
        choices=("NOT_DELIVERED_ONLY", "REDELIVERY_ONLY", "NOT_DELIVERED_AND_REDELIVERY", "ALL"),
        default="NOT_DELIVERED_ONLY",
    )
    parser.add_argument(
        "--same-exam-date-policy",
        choices=("LATEST_DOWNLOAD", "EARLIEST_DOWNLOAD", "MANUAL_REVIEW"),
        default="LATEST_DOWNLOAD",
    )
    parser.add_argument("--grouping-mode", choices=("ALL", "BY_FACILITY"), default="ALL")
    parser.add_argument("--sender-code", default=DEFAULT_SENDER_CODE)
    parser.add_argument("--sender-name", default=None)
    parser.add_argument("--created-by", default=None)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Apply changes. Without this flag the script runs as dry-run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.confirm
    config = FundDeliveryListConfig(
        event_id=args.event_id,
        insurer_number=args.insurer_number,
        list_name=args.list_name or default_list_name(args.exam_month, args.output_mode),
        output_mode=args.output_mode,
        exam_month=args.exam_month,
        delivery_policy=args.delivery_policy,
        same_exam_date_policy=args.same_exam_date_policy,
        grouping_mode=args.grouping_mode,
        sender_code=args.sender_code,
        sender_name=args.sender_name,
        created_by=args.created_by,
        dry_run=dry_run,
    )

    mysql_params = load_mysql_base_params()
    with connect_ctx(mysql_params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        run_id = start_run(
            cur,
            phase="HIA_CREATE_FUND_DELIVERY_LIST",
            source="HIA",
            db_schema=args.database,
            db_path=None,
            input_base=None,
            input_file=None,
            insurer_number=args.insurer_number,
            dry_run=dry_run,
            limit_rows=None,
        )

        try:
            summary = build_fund_delivery_list(cur, config)
            metrics = RunMetrics()
            metrics.rows_seen = summary.valid_xmls_seen
            metrics.rows_inserted = summary.list_members_inserted + summary.list_created
            metrics.rows_updated = summary.candidates_upserted + summary.person_status_upserted
            metrics.rows_skipped = summary.skipped_by_delivery_policy

            if dry_run:
                conn.rollback()
                status_override = "success"
            else:
                status_override = None

            finish_run(
                cur,
                run_id,
                metrics,
                status_override=status_override,
                extra_notes=(
                    f"list_id={summary.list_id} "
                    f"candidate_groups={summary.candidate_groups_seen} "
                    f"selected={summary.selected_candidates} "
                    f"not_selected={summary.not_selected_candidates} "
                    f"review_required={summary.review_required_candidates} "
                    f"members={summary.list_members_seen}"
                ),
            )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(
        "create_fund_delivery_list "
        f"dry_run={1 if dry_run else 0} list_id={summary.list_id} "
        f"valid_xmls={summary.valid_xmls_seen} groups={summary.candidate_groups_seen} "
        f"candidates={summary.candidates_upserted} selected={summary.selected_candidates} "
        f"not_selected={summary.not_selected_candidates} review_required={summary.review_required_candidates} "
        f"members={summary.list_members_seen} skipped_by_policy={summary.skipped_by_delivery_policy}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
