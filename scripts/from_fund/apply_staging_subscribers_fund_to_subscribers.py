#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from typing import Any, Mapping, cast

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR
from scripts.lib.etl.metrics import RunMetrics
from scripts.lib.etl.runs import finish_run, start_run
from scripts.from_fund.script_lib.apply_subscribers_fund_name_parts import (
    apply_name_parts_from_staging_subscribers_fund,
)

from scripts.from_fund.script_lib.parts_apply_refresh import (
    REFRESH_MODE_MATCHED,
    refresh_parts_apply_rows,
)

ETL_PHASE = "apply"
ETL_SOURCE = "staging_subscribers_fund"


def fetch_matched_parts_apply_target_rows(conn: Any, *, import_run_id: int) -> list[dict[str, Any]]:
    """通常 import 後の parts_apply refresh 対象行を取得する。

    対象選定は呼び元責務。
    本処理では import 時点で matched_subscriber_id が解決済みの行だけを
    parts_apply_subscriber_id 解決候補にする。
    """
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              id,
              identity_hash,
              matched_subscriber_id
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE import_run_id = %s
              AND matched_subscriber_id IS NOT NULL
              AND parts_apply_subscriber_id IS NULL
              AND parts_apply_status IS NULL
            ORDER BY id
            """,
            (import_run_id,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, required=True, help="import 側の run_id")
    args = parser.parse_args()

    run_id = args.run_id
    base_params = load_mysql_base_params()

    with connect_ctx(base_params, database=DEV_PHR, autocommit=False) as conn, connect_ctx(
        base_params, database=DEV_PHR, autocommit=True
    ) as etl_conn:
        metrics = RunMetrics()

        run_cur = etl_conn.cursor()
        try:
            apply_run_id = start_run(
                run_cur,
                phase=ETL_PHASE,
                source=ETL_SOURCE,
                db_schema=DEV_PHR,
                db_path=f"{base_params.host}:{base_params.port}/{DEV_PHR}",
                input_base="apply_staging_subscribers_fund_to_subscribers",
                input_file=f"run_id={run_id}",
                insurer_number=None,
                dry_run=False,
                limit_rows=None,
            )
        finally:
            run_cur.close()

        status = "success"
        try:
            refresh_rows = fetch_matched_parts_apply_target_rows(
                conn,
                import_run_id=run_id,
            )
            refresh_result = refresh_parts_apply_rows(
                conn=conn,
                rows=refresh_rows,
                dry_run=False,
                mode=REFRESH_MODE_MATCHED,
            )
            print(f"parts_apply_refresh: {refresh_result}")
            result = apply_name_parts_from_staging_subscribers_fund(
                conn,
                run_id,
                audit_source="apply_staging_subscribers_fund_to_subscribers",
                change_run_id=apply_run_id,
            )

            metrics.rows_seen = result.rows_seen_count
            metrics.rows_inserted = result.rows_updated_count
            metrics.rows_skipped = result.rows_skipped_count
            metrics.errors = result.row_error_count

            if result.row_error_count > 0:
                status = "partial"

            conn.commit()

        except Exception:
            status = "failed"
            conn.rollback()
            raise
        finally:
            finish_cur = etl_conn.cursor()
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
        print(f"import_run_id: {run_id}")
        print(f"apply_run_id: {apply_run_id}")
        print(f"rows_seen: {metrics.rows_seen}")
        print(f"rows_updated: {metrics.rows_inserted}")
        print(f"rows_skipped: {metrics.rows_skipped}")
        print(f"errors: {metrics.errors}")


if __name__ == "__main__":
    main()