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

import yaml

# repo root を import path に追加する。
# scripts/hia/apply_hia_subscriber_sync.py -> repo root は parents[2]
REPO_ROOT: Path = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

from scripts.lib.etl import RunMetrics, finish_run, start_run

from scripts.hia.script_lib.hub_subscriber_prepare import (
    prepare_hia_subscriber_apply_actions,
)
from scripts.hia.script_lib.hub_subscriber_compare import (
    compare_hia_subscriber_apply_actions,
)
from scripts.hia.script_lib.hub_subscriber_apply import apply_hia_subscriber_rows


def load_apply_config() -> dict:
    config_path = REPO_ROOT / "scripts" / "hia" / "config" / "apply_staging_to_subscribers.yml"

    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp)

    if not isinstance(data, dict):
        return {}

    return data


def get_config_value(config: dict, key: str, default):
    value = config.get(key, default)
    return default if value is None else value


def resolve_import_run_id(cur, import_run_id_value) -> int:
    """import_run_id: auto の場合、未処理 staging の最新 import_run_id を使う。"""

    text = str(import_run_id_value).strip().lower()

    if text in ("", "0", "auto", "latest"):
        cur.execute(
            """
            SELECT import_run_id
            FROM staging_subscribers_hub
            WHERE processed_run_id IS NULL
              AND import_run_id IS NOT NULL
            GROUP BY import_run_id
            ORDER BY import_run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(
                "No unprocessed staging_subscribers_hub rows found for auto import_run_id."
            )
        return int(row["import_run_id"])

    try:
        import_run_id = int(text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid import_run_id: {import_run_id_value!r}. Use a positive integer or 'auto'."
        ) from exc

    if import_run_id <= 0:
        raise ValueError(
            f"Invalid import_run_id: {import_run_id_value!r}. Use a positive integer or 'auto'."
        )

    return import_run_id


# ============================================================
# args
# ============================================================


def parse_args() -> argparse.Namespace:
    config = load_apply_config()

    parser = argparse.ArgumentParser(
        description="Apply HIA subscriber staging rows to target tables."
    )

    parser.add_argument(
        "--import-run-id",
        default=get_config_value(config, "import_run_id", "auto"),
        help="対象の import_run_id。数値または auto。auto の場合は未処理 staging の最新 import_run_id を使う。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=get_config_value(config, "limit", 0),
        help="処理件数上限。0 の場合は無制限。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=get_config_value(config, "dry_run", False),
        help="target tables の更新を行わず、apply候補の処理確認のみ行う。prepare/compare/applyによるDB変更は rollback される。",
    )
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        default=get_config_value(config, "skip_prepare", False),
        help="prepare phase をスキップする。既にprepare済みの場合に使用する。",
    )
    parser.add_argument(
        "--skip-compare",
        action="store_true",
        default=get_config_value(config, "skip_compare", False),
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

    params = load_mysql_base_params()

    apply_run_id = None
    metrics_all = RunMetrics()

    with connect_ctx(params, database=DEV_PHR) as conn:
        with dict_cursor(conn) as cur:
            try:
                import_run_id = resolve_import_run_id(cur, args.import_run_id)

                apply_run_id = start_run(
                    cur,
                    phase="apply",
                    source="apply_hia_subscriber_sync",
                    db_schema=DEV_PHR,
                    db_path=None,
                    input_base=None,
                    input_file=None,
                    insurer_number=None,
                    dry_run=args.dry_run,
                    limit_rows=args.limit if args.limit > 0 else None,
                )
                conn.commit()

                print("=== HIA subscriber apply start ===")
                print(f"apply_run_id  : {apply_run_id}")
                print(f"import_run_id : {import_run_id}")
                print(f"limit         : {args.limit}")
                print(f"dry_run       : {args.dry_run}")

                if not args.skip_prepare:
                    prepare_metrics = prepare_hia_subscriber_apply_actions(
                        cur,
                        import_run_id=import_run_id,
                        limit=args.limit,
                        dry_run=args.dry_run,
                    )
                    metrics_all.rows_seen += getattr(prepare_metrics, "rows_seen", 0)
                    metrics_all.rows_inserted += getattr(prepare_metrics, "rows_updated", 0)
                    metrics_all.rows_skipped += getattr(prepare_metrics, "rows_skipped", 0)
                    metrics_all.errors += getattr(prepare_metrics, "rows_error", 0)
                    print_metrics("prepare", prepare_metrics)

                if not args.skip_compare:
                    compare_metrics = compare_hia_subscriber_apply_actions(
                        cur,
                        import_run_id=import_run_id,
                        limit=args.limit,
                        dry_run=args.dry_run,
                    )
                    metrics_all.rows_seen += getattr(compare_metrics, "rows_seen", 0)
                    metrics_all.rows_inserted += getattr(compare_metrics, "rows_updated", 0)
                    metrics_all.rows_skipped += getattr(compare_metrics, "rows_skipped", 0)
                    metrics_all.errors += getattr(compare_metrics, "rows_error", 0)
                    print_metrics("compare", compare_metrics)

                apply_metrics = apply_hia_subscriber_rows(
                    cur,
                    import_run_id=import_run_id,
                    apply_run_id=apply_run_id,
                    limit=args.limit,
                    dry_run=args.dry_run,
                )
                metrics_all.rows_seen += getattr(apply_metrics, "rows_seen", 0)
                metrics_all.rows_inserted += getattr(apply_metrics, "rows_applied", 0)
                metrics_all.rows_skipped += getattr(apply_metrics, "rows_noop", 0) + getattr(apply_metrics, "rows_review_skipped", 0)
                metrics_all.errors += getattr(apply_metrics, "rows_error", 0)
                print_metrics("apply", apply_metrics)

                if args.dry_run:
                    conn.rollback()
                    finish_run(
                        cur,
                        apply_run_id,
                        metrics_all,
                        status_override="success",
                        extra_notes="dry-run completed; rolled back",
                    )
                    conn.commit()
                else:
                    finish_run(cur, apply_run_id, metrics_all)
                    conn.commit()

                print("\n=== HIA subscriber apply finished ===")
                return 0

            except Exception as exc:
                conn.rollback()

                if apply_run_id is not None:
                    metrics_all.errors += 1
                    finish_run(
                        cur,
                        apply_run_id,
                        metrics_all,
                        status_override="failed",
                        extra_notes=f"{type(exc).__name__}: {exc}",
                    )
                    conn.commit()

                print("\n[ERROR] HIA subscriber apply failed")
                print(f"{type(exc).__name__}: {exc}")
                return 1


if __name__ == "__main__":
    raise SystemExit(main())