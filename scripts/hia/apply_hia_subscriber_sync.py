

# -*- coding: utf-8 -*-
"""
============================================================
Module : apply_hia_subscriber_sync.py
Path   : scripts/hia/apply_hia_subscriber_sync.py
Project: PHR

Purpose:
    Apply HIA subscriber staging rows to target tables.

Responsibility:
    - CLI entry point for HIA subscriber apply orchestration
    - start / finish apply run
    - execute prepare -> compare -> apply in order
    - keep this file thin; implementation lives in script_lib modules

Non-goals:
    - CSV import
    - current snapshot hydrate
    - detailed SQL implementation for subscriber/address/contact point apply
    - field-level audit implementation

Flow:
    staging_subscribers_hub
      ↓
    prepare_hia_subscriber_apply_actions
      ↓
    compare_hia_subscriber_apply_actions
      ↓
    apply_hia_subscriber_rows

Notes:
    field-level subscriber_audit is written inside apply_action_* modules.
============================================================
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from pathlib import Path

# repo root を import path に追加する。
# scripts/hia/apply_hia_subscriber_sync.py -> repo root は parents[2]
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.work_folder.lib.db import get_connection
from scripts.work_folder.lib.etl import finish_run, start_run

from scripts.hia.script_lib.hub_subscriber_prepare import (
    prepare_hia_subscriber_apply_actions,
)
from scripts.hia.script_lib.hub_subscriber_compare import (
    compare_hia_subscriber_apply_actions,
)
from scripts.hia.script_lib.hub_subscriber_apply import apply_hia_subscriber_rows


# ============================================================
# args
# ============================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply HIA subscriber staging rows to target tables."
    )

    parser.add_argument(
        "--import-run-id",
        type=int,
        required=True,
        help="対象の import_run_id。staging_subscribers_hub.import_run_id を指定する。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="処理件数上限。0 の場合は無制限。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB更新を行わず、apply候補の処理確認のみ行う。prepare/compare/applyの更新も行わない。",
    )
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="prepare phase をスキップする。既にprepare済みの場合に使用する。",
    )
    parser.add_argument(
        "--skip-compare",
        action="store_true",
        help="compare phase をスキップする。既にcompare済みの場合に使用する。",
    )

    return parser.parse_args()


# ============================================================
# logging helpers
# ============================================================


def print_metrics(label: str, metrics) -> None:
    print(f"\n[{label}]")
    for key, value in asdict(metrics).items():
        print(f"  {key}: {value}")


# ============================================================
# main
# ============================================================


def main() -> int:
    args = parse_args()

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    apply_run_id = None

    try:
        apply_run_id = start_run(
            cur,
            run_type="hia_subscriber_apply",
            source="apply_hia_subscriber_sync",
            note=(
                f"import_run_id={args.import_run_id}; "
                f"limit={args.limit}; dry_run={args.dry_run}; "
                f"skip_prepare={args.skip_prepare}; skip_compare={args.skip_compare}"
            ),
        )
        conn.commit()

        print("=== HIA subscriber apply start ===")
        print(f"apply_run_id  : {apply_run_id}")
        print(f"import_run_id : {args.import_run_id}")
        print(f"limit         : {args.limit}")
        print(f"dry_run       : {args.dry_run}")

        if not args.skip_prepare:
            prepare_metrics = prepare_hia_subscriber_apply_actions(
                cur,
                import_run_id=args.import_run_id,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            print_metrics("prepare", prepare_metrics)

        if not args.skip_compare:
            compare_metrics = compare_hia_subscriber_apply_actions(
                cur,
                import_run_id=args.import_run_id,
                limit=args.limit,
                dry_run=args.dry_run,
            )
            print_metrics("compare", compare_metrics)

        apply_metrics = apply_hia_subscriber_rows(
            cur,
            import_run_id=args.import_run_id,
            apply_run_id=apply_run_id,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        print_metrics("apply", apply_metrics)

        if args.dry_run:
            conn.rollback()
            finish_run(
                cur,
                apply_run_id,
                status="dry_run",
                message="dry-run completed; rolled back",
            )
            conn.commit()
        else:
            finish_run(
                cur,
                apply_run_id,
                status="success",
                message="apply completed",
            )
            conn.commit()

        print("\n=== HIA subscriber apply finished ===")
        return 0

    except Exception as exc:
        conn.rollback()

        if apply_run_id is not None:
            finish_run(
                cur,
                apply_run_id,
                status="error",
                message=f"{type(exc).__name__}: {exc}",
            )
            conn.commit()

        print("\n[ERROR] HIA subscriber apply failed")
        print(f"{type(exc).__name__}: {exc}")
        return 1

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())