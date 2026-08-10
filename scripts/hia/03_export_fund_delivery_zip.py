#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any, Mapping

if __name__ == "__main__" and __package__ is None:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from scripts.hia.script_lib.fund_delivery_zip_exporter import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    DEFAULT_XSD_DIR,
    FundDeliveryZipExportConfig,
    export_fund_delivery_zip,
)
from scripts.hia.script_lib.config_loader import config_bool, config_value, load_yaml_config  # noqa: E402
from scripts.lib.db.config import load_mysql_base_params  # noqa: E402
from scripts.lib.db.mysql import connect_ctx, dict_cursor  # noqa: E402
from scripts.lib.etl.metrics import RunMetrics  # noqa: E402
from scripts.lib.etl.runs import finish_run, start_run  # noqa: E402


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "fund_delivery.yml"


def _path_from_config(value: Any, default: Path) -> Path:
    if value in (None, ""):
        return default
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[2] / path


def resolve_delivery_list_id(cur: Any, requested_id: int | None) -> int:
    if requested_id is not None:
        return requested_id
    cur.execute(
        """
        SELECT delivery_list_id
          FROM fund_delivery_lists
         WHERE list_status IN ('READY', 'CREATED')
         ORDER BY delivery_list_id DESC
         LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(
            "delivery_list_id is required. Set export.delivery_list_id in fund_delivery.yml "
            "or create a READY fund_delivery_list first."
        )
    return int(row["delivery_list_id"])


def parse_args() -> argparse.Namespace:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    known, _ = bootstrap.parse_known_args()
    data = load_yaml_config(Path(known.config))
    section = data.get("export") or {}
    if not isinstance(section, Mapping):
        raise ValueError("export must be a mapping in fund_delivery.yml")
    parser = argparse.ArgumentParser(
        description="Export a fund delivery ZIP from a fund_delivery_lists record.",
        parents=[bootstrap],
    )
    parser.add_argument("--database", default=config_value(data, "database", "health_exam_result"))
    delivery_list_id = config_value(section, "delivery_list_id", None)
    parser.add_argument("--delivery-list-id", type=int, default=None if delivery_list_id in (None, "") else int(delivery_list_id))
    parser.add_argument("--output-base-dir", type=Path, default=_path_from_config(section.get("output_base_dir"), DEFAULT_OUTPUT_DIR))
    parser.add_argument("--xsd-dir", type=Path, default=_path_from_config(section.get("xsd_dir"), DEFAULT_XSD_DIR))
    parser.add_argument("--delivery-date", default=config_value(section, "delivery_date", None), help="YYYYMMDD. Defaults to today.")
    parser.add_argument("--output-seq", type=int, default=int(config_value(section, "output_seq", 0)), help="MHLW-style output sequence digit.")
    parser.add_argument("--send-seq", type=int, default=int(config_value(section, "send_seq", 1)), help="MHLW-style send sequence.")
    parser.add_argument("--created-by", default=config_value(section, "created_by", None))
    parser.add_argument(
        "--confirm",
        action="store_true",
        default=config_bool(section, "confirm", False),
        help="Create ZIP and DB run/member records. Without this flag the script runs as dry-run.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mysql_params = load_mysql_base_params()
    with connect_ctx(mysql_params, database=args.database, autocommit=False) as conn:
        cur = dict_cursor(conn)
        delivery_list_id = resolve_delivery_list_id(cur, args.delivery_list_id)
        dry_run = not args.confirm
        config = FundDeliveryZipExportConfig(
            delivery_list_id=delivery_list_id,
            output_base_dir=args.output_base_dir,
            xsd_dir=args.xsd_dir,
            delivery_date=args.delivery_date,
            output_seq=args.output_seq,
            send_seq=args.send_seq,
            created_by=args.created_by,
            dry_run=dry_run,
        )
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
