#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from typing import Any

from scripts.lib.db.mysql import connect_mysql
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.etl.runs import start_run, finish_run
from scripts.lib.etl.metrics import RunMetrics

from scripts.from_fund.script_lib.apply_subscribers_fund_name_parts import (
    apply_name_parts_from_staging_subscribers_fund,
)

ETL_PHASE = "apply"
ETL_SOURCE = "staging_subscribers_fund"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, required=True, help="import 側の run_id")
    args = parser.parse_args()

    run_id = args.run_id

    base_params = load_mysql_base_params()

    conn = connect_mysql(base_params, database=DEV_PHR)

    metrics = RunMetrics()

    run_cur = conn.cursor()
    try:
        apply_run_id = start_run(
            run_cur,
            phase=ETL_PHASE,
            source=ETL_SOURCE,
            db_schema=DEV_PHR,
            db_path=f"{base_params.host}:{base_params.port}/{DEV_PHR}",
            input_base="apply",
            input_file=str(run_id),
            insurer_number=None,
            dry_run=False,
            limit_rows=None,
        )
    finally:
        run_cur.close()

    try:
        result = apply_name_parts_from_staging_subscribers_fund(
            conn,
            run_id,
        )

        metrics.rows_seen = result.rows_seen_count
        metrics.rows_inserted = result.rows_updated_count
        metrics.rows_skipped = result.rows_skipped_count
        metrics.errors = result.row_error_count

        status = "success"
        if result.row_error_count > 0:
            status = "partial"

    except Exception as e:
        status = "failed"
        pass
        raise
    finally:
        finish_cur = conn.cursor()
        try:
            finish_run(
                finish_cur,
                apply_run_id,
                metrics,
                status_override=status,
            )
        finally:
            finish_cur.close()

    print("=== apply result ===")
    print(f"run_id: {run_id}")
    print(f"rows_seen: {metrics.rows_seen}")
    print(f"rows_updated: {metrics.rows_inserted}")
    print(f"rows_skipped: {metrics.rows_skipped}")
    print(f"errors: {metrics.errors}")


if __name__ == "__main__":
    main()