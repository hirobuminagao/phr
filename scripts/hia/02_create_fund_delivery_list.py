#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Mapping

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.fund_delivery_list_builder import (  # noqa: E402
    FundDeliveryListConfig,
    build_fund_delivery_list,
)
from scripts.hia.script_lib.config_loader import config_bool, config_value, load_yaml_config  # noqa: E402
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


DEFAULT_INSURER_NUMBER = "06139463"
DEFAULT_SENDER_CODE = "1322100106"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "fund_delivery.yml"


def default_list_name(exam_month: str | None, output_mode: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_mode == "EXAM_MONTH":
        return f"{exam_month}_健保納品リスト_{stamp}"
    return f"全件_健保納品リスト_{stamp}"


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _list_name(base_name: str | None, exam_month: str | None, output_mode: str, total_months: int) -> str:
    if base_name and output_mode == "EXAM_MONTH" and total_months > 1:
        return f"{exam_month}_{base_name}"
    return base_name or default_list_name(exam_month, output_mode)


def parse_args() -> argparse.Namespace:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    known, _ = bootstrap.parse_known_args()
    data = load_yaml_config(Path(known.config))
    section = data.get("list") or {}
    if not isinstance(section, Mapping):
        raise ValueError("list must be a mapping in fund_delivery.yml")
    parser = argparse.ArgumentParser(
        description="Create a fund delivery output list from imported HIA XML ledgers.",
        parents=[bootstrap],
    )
    parser.add_argument("--database", default=config_value(data, "database", "health_exam_result"))
    event_id = config_value(data, "event_id", None)
    parser.add_argument("--event-id", type=int, default=None if event_id in (None, "") else int(event_id))
    parser.add_argument("--insurer-number", default=str(config_value(data, "insurer_number", DEFAULT_INSURER_NUMBER)))
    parser.add_argument("--list-name", default=config_value(section, "list_name", None))
    parser.add_argument("--output-mode", choices=("EXAM_MONTH", "ALL"), default=str(config_value(section, "output_mode", "EXAM_MONTH")))
    parser.add_argument("--exam-month", action="append", default=None, help="YYYYMM. Can be repeated. Required when output-mode=EXAM_MONTH.")
    parser.set_defaults(config_exam_month=config_value(section, "exam_month", None))
    parser.set_defaults(config_exam_months=section.get("exam_months"))
    parser.add_argument(
        "--delivery-policy",
        choices=("NOT_DELIVERED_ONLY", "REDELIVERY_ONLY", "NOT_DELIVERED_AND_REDELIVERY", "ALL"),
        default=str(config_value(section, "delivery_policy", "NOT_DELIVERED_ONLY")),
    )
    parser.add_argument(
        "--same-exam-date-policy",
        choices=("LATEST_DOWNLOAD", "EARLIEST_DOWNLOAD", "MANUAL_REVIEW"),
        default=str(config_value(section, "same_exam_date_policy", "LATEST_DOWNLOAD")),
    )
    parser.add_argument("--grouping-mode", choices=("ALL", "BY_FACILITY"), default=str(config_value(section, "grouping_mode", "ALL")))
    parser.add_argument("--sender-code", default=str(config_value(section, "sender_code", DEFAULT_SENDER_CODE)))
    parser.add_argument("--sender-name", default=config_value(section, "sender_name", None))
    parser.add_argument("--created-by", default=config_value(section, "created_by", None))
    parser.add_argument(
        "--confirm",
        action="store_true",
        default=config_bool(section, "confirm", False),
        help="Apply changes. Without this flag the script runs as dry-run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.confirm
    if args.output_mode == "EXAM_MONTH":
        exam_months = args.exam_month or _string_list(args.config_exam_months) or _string_list(args.config_exam_month)
        if not exam_months:
            raise ValueError("exam_month or exam_months is required when output-mode=EXAM_MONTH")
    else:
        exam_months = [None]

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
            summaries = []
            for exam_month in exam_months:
                config = FundDeliveryListConfig(
                    event_id=args.event_id,
                    insurer_number=args.insurer_number,
                    list_name=_list_name(args.list_name, exam_month, args.output_mode, len(exam_months)),
                    output_mode=args.output_mode,
                    exam_month=exam_month,
                    delivery_policy=args.delivery_policy,
                    same_exam_date_policy=args.same_exam_date_policy,
                    grouping_mode=args.grouping_mode,
                    sender_code=args.sender_code,
                    sender_name=args.sender_name,
                    created_by=args.created_by,
                    dry_run=dry_run,
                )
                summaries.append(build_fund_delivery_list(cur, config))

            metrics = RunMetrics()
            metrics.rows_seen = sum(item.valid_xmls_seen for item in summaries)
            metrics.rows_inserted = sum(item.list_members_inserted + item.list_created for item in summaries)
            metrics.rows_updated = sum(item.candidates_upserted + item.person_status_upserted for item in summaries)
            metrics.rows_skipped = sum(item.skipped_by_delivery_policy for item in summaries)

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
                    f"list_ids={','.join(str(item.list_id) for item in summaries)} "
                    f"months={','.join(str(month) for month in exam_months if month)} "
                    f"candidate_groups={sum(item.candidate_groups_seen for item in summaries)} "
                    f"selected={sum(item.selected_candidates for item in summaries)} "
                    f"not_selected={sum(item.not_selected_candidates for item in summaries)} "
                    f"review_required={sum(item.review_required_candidates for item in summaries)} "
                    f"members={sum(item.list_members_seen for item in summaries)}"
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
        f"dry_run={1 if dry_run else 0} "
        f"list_ids={','.join(str(item.list_id) for item in summaries)} "
        f"months={','.join(str(month) for month in exam_months if month)} "
        f"valid_xmls={sum(item.valid_xmls_seen for item in summaries)} "
        f"groups={sum(item.candidate_groups_seen for item in summaries)} "
        f"candidates={sum(item.candidates_upserted for item in summaries)} "
        f"selected={sum(item.selected_candidates for item in summaries)} "
        f"not_selected={sum(item.not_selected_candidates for item in summaries)} "
        f"review_required={sum(item.review_required_candidates for item in summaries)} "
        f"members={sum(item.list_members_seen for item in summaries)} "
        f"skipped_by_policy={sum(item.skipped_by_delivery_policy for item in summaries)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
