#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.fund_delivery_zip_exporter import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    DEFAULT_XSD_DIR,
    FundDeliveryZipExportConfig,
    export_fund_delivery_zip,
)
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a fund delivery ZIP from a fund_delivery_lists record.",
    )
    parser.add_argument("--database", default="health_exam_result")
    parser.add_argument("--delivery-list-id", type=int, required=True)
    parser.add_argument("--output-base-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--xsd-dir", type=Path, default=DEFAULT_XSD_DIR)
    parser.add_argument("--delivery-date", default=None, help="YYYYMMDD. Defaults to today.")
    parser.add_argument("--output-seq", type=int, default=0, help="MHLW-style output sequence digit.")
    parser.add_argument("--send-seq", type=int, default=1, help="MHLW-style send sequence.")
    parser.add_argument("--created-by", default=None)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Create ZIP and DB run/member records. Without this flag the script runs as dry-run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.confirm
    config = FundDeliveryZipExportConfig(
        delivery_list_id=args.delivery_list_id,
        output_base_dir=args.output_base_dir,
        xsd_dir=args.xsd_dir,
        delivery_date=args.delivery_date,
        output_seq=args.output_seq,
        send_seq=args.send_seq,
        created_by=args.created_by,
        dry_run=dry_run,
    )

    mysql_params = load_mysql_base_params()
    with connect_ctx(mysql_params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        run_id = start_run(
            cur,
            phase="HIA_EXPORT_FUND_DELIVERY_ZIP",
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
            summary = export_fund_delivery_zip(cur, config=config, etl_run_id=run_id)
            metrics = RunMetrics()
            metrics.rows_seen = summary.members_seen
            metrics.rows_inserted = summary.members_written
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
                    f"delivery_run_id={summary.delivery_run_id} "
                    f"source_zip_count={summary.source_zip_count} "
                    f"report_category_10_count={summary.report_category_10_count} "
                    f"output_zip={summary.output_zip_name}"
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
        "export_fund_delivery_zip "
        f"dry_run={1 if dry_run else 0} list_id={summary.delivery_list_id} "
        f"run_id={summary.delivery_run_id} members={summary.members_seen} "
        f"written={summary.members_written} source_zips={summary.source_zip_count} "
        f"output_zip={summary.output_zip_name} output_path={summary.output_zip_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
