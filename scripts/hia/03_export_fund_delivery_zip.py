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


def _int_list(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, int):
        return [value]
    if isinstance(value, str):
        return [int(part.strip()) for part in value.split(",") if part.strip()]
    if isinstance(value, list | tuple):
        return [int(item) for item in value]
    raise ValueError(f"Expected integer list, got {type(value).__name__}")


def resolve_delivery_list_ids(cur: Any, requested_ids: list[int]) -> list[int]:
    if requested_ids:
        return requested_ids
    cur.execute(
        """
        SELECT delivery_list_id
          FROM fund_delivery_lists
         WHERE list_status IN ('READY', 'CREATED')
         ORDER BY
           CASE WHEN exam_month IS NULL THEN 1 ELSE 0 END,
           exam_month,
           delivery_list_id
        """
    )
    rows = list(cur.fetchall() or [])
    if not rows:
        raise ValueError(
            "delivery_list_id is required. Set export.delivery_list_id in fund_delivery.yml "
            "or create READY fund_delivery_lists first."
        )
    return [int(row["delivery_list_id"]) for row in rows]


def resolve_delivery_list_id(cur: Any, requested_id: int | None) -> int:
    if requested_id is not None:
        return requested_id
    return resolve_delivery_list_ids(cur, [])[0]


def _delivery_date(value: str | None) -> str:
    if value:
        return value
    from datetime import date

    return date.today().strftime("%Y%m%d")


def next_send_seq(
    cur: Any,
    *,
    sender_code: str,
    insurer_number: str,
    delivery_date: str,
    output_seq: int,
) -> int:
    prefix = f"{sender_code}_{insurer_number}_{delivery_date}{output_seq}_"
    cur.execute(
        """
        SELECT output_zip_name
          FROM fund_delivery_runs
         WHERE output_zip_name LIKE %s
        """,
        (prefix + "%.zip",),
    )
    max_seq = 0
    for row in cur.fetchall() or []:
        name = str(row["output_zip_name"])
        if not name.startswith(prefix) or not name.endswith(".zip"):
            continue
        seq_text = name[len(prefix) : -4]
        if seq_text.isdigit():
            max_seq = max(max_seq, int(seq_text))
    return max_seq + 1


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
    parser.add_argument("--delivery-list-id", type=int, action="append", default=_int_list(section.get("delivery_list_ids")) or _int_list(delivery_list_id))
    parser.add_argument("--output-base-dir", type=Path, default=_path_from_config(section.get("output_base_dir"), DEFAULT_OUTPUT_DIR))
    parser.add_argument("--xsd-dir", type=Path, default=_path_from_config(section.get("xsd_dir"), DEFAULT_XSD_DIR))
    parser.add_argument("--delivery-date", default=config_value(section, "delivery_date", None), help="YYYYMMDD. Defaults to today.")
    parser.add_argument("--output-seq", type=int, default=int(config_value(section, "output_seq", 0)), help="MHLW-style output sequence digit.")
    parser.add_argument("--send-seq", default=str(config_value(section, "send_seq", 1)), help="MHLW-style send sequence. Use 'auto' to increment per output ZIP.")
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
        delivery_list_ids = resolve_delivery_list_ids(cur, args.delivery_list_id)
        dry_run = not args.confirm
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
            summaries = []
            auto_send_seq = str(args.send_seq).strip().lower() == "auto"
            next_auto_send_seq: int | None = None
            for delivery_list_id in delivery_list_ids:
                list_row = None
                send_seq: int
                if auto_send_seq:
                    from scripts.hia.script_lib.fund_delivery_zip_exporter import load_delivery_list

                    list_row = load_delivery_list(cur, delivery_list_id)
                    delivery_date = _delivery_date(args.delivery_date)
                    if next_auto_send_seq is None:
                        next_auto_send_seq = next_send_seq(
                            cur,
                            sender_code=str(list_row["sender_code"]),
                            insurer_number=str(list_row["insurer_number"]),
                            delivery_date=delivery_date,
                            output_seq=args.output_seq,
                        )
                    send_seq = next_auto_send_seq
                    next_auto_send_seq += 1
                else:
                    send_seq = int(args.send_seq)

                config = FundDeliveryZipExportConfig(
                    delivery_list_id=delivery_list_id,
                    output_base_dir=args.output_base_dir,
                    xsd_dir=args.xsd_dir,
                    delivery_date=args.delivery_date,
                    output_seq=args.output_seq,
                    send_seq=send_seq,
                    created_by=args.created_by,
                    dry_run=dry_run,
                )
                summaries.append(export_fund_delivery_zip(cur, config=config, etl_run_id=run_id))
            metrics = RunMetrics()
            metrics.rows_seen = sum(item.members_seen for item in summaries)
            metrics.rows_inserted = sum(item.members_written for item in summaries)
            metrics.errors = sum(item.errors for item in summaries)

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
                    f"delivery_list_ids={','.join(str(item.delivery_list_id) for item in summaries)} "
                    f"delivery_run_ids={','.join(str(item.delivery_run_id) for item in summaries)} "
                    f"source_zip_count={sum(item.source_zip_count for item in summaries)} "
                    f"report_category_10_count={sum(item.report_category_10_count for item in summaries)} "
                    f"output_zips={','.join(str(item.output_zip_name) for item in summaries)}"
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
        f"dry_run={1 if dry_run else 0} "
        f"list_ids={','.join(str(item.delivery_list_id) for item in summaries)} "
        f"run_ids={','.join(str(item.delivery_run_id) for item in summaries)} "
        f"members={sum(item.members_seen for item in summaries)} "
        f"written={sum(item.members_written for item in summaries)} "
        f"source_zips={sum(item.source_zip_count for item in summaries)} "
        f"output_zips={','.join(str(item.output_zip_name) for item in summaries)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
