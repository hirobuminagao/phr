#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.fund_delivery_submission_marker import (  # noqa: E402
    FundDeliverySubmissionConfig,
    mark_fund_delivery_submitted,
)
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


def parse_submitted_at(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("submitted-at must be YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, or YYYY-MM-DDTHH:MM:SS")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mark fund delivery members as submitted/error/pending.",
    )
    parser.add_argument("--database", default="health_exam_result")
    parser.add_argument("--delivery-list-id", type=int, required=True)
    parser.add_argument("--delivery-member-id", type=int, action="append", default=[])
    parser.add_argument("--all", dest="all_members", action="store_true", help="Update all members in the list.")
    parser.add_argument(
        "--status",
        choices=("SUBMITTED", "SUBMISSION_ERROR", "PENDING"),
        default="SUBMITTED",
    )
    parser.add_argument("--submitted-at", type=parse_submitted_at, default=None)
    parser.add_argument("--submitted-by", default=None)
    parser.add_argument("--note", default=None)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Apply changes. Without this flag the script runs as dry-run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.confirm
    config = FundDeliverySubmissionConfig(
        delivery_list_id=args.delivery_list_id,
        delivery_member_ids=tuple(args.delivery_member_id),
        all_members=args.all_members,
        target_status=args.status,
        submitted_at=args.submitted_at,
        submitted_by=args.submitted_by,
        submission_note=args.note,
        dry_run=dry_run,
    )

    mysql_params = load_mysql_base_params()
    with connect_ctx(mysql_params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        run_id = start_run(
            cur,
            phase="HIA_MARK_FUND_DELIVERY_SUBMITTED",
            source="HIA",
            db_schema=args.database,
            db_path=None,
            input_base=None,
            input_file=None,
            insurer_number=None,
            dry_run=dry_run,
            limit_rows=None,
        )

        try:
            summary = mark_fund_delivery_submitted(cur, config)
            metrics = RunMetrics()
            metrics.rows_seen = summary.members_seen
            metrics.rows_updated = summary.members_updated + summary.runs_updated + summary.person_status_updated
            metrics.errors = summary.errors

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
                    f"delivery_list_id={summary.delivery_list_id} "
                    f"target_status={args.status} "
                    f"members_updated={summary.members_updated} "
                    f"runs_updated={summary.runs_updated} "
                    f"person_status_updated={summary.person_status_updated} "
                    f"list_status={summary.list_status}"
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
        "mark_fund_delivery_submitted "
        f"dry_run={1 if dry_run else 0} list_id={summary.delivery_list_id} "
        f"status={args.status} members={summary.members_seen} "
        f"members_updated={summary.members_updated} runs_updated={summary.runs_updated} "
        f"person_status_updated={summary.person_status_updated} list_status={summary.list_status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
