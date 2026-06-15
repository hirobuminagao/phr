

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
backfill_name_parts_after_hub_apply.py

Hub apply 後に subscribers へ作成された加入者を対象に、
staging_subscribers_fund の name parts を後追い補完する。

処理:
1. parts_apply_subscriber_id / status が未設定の staging 行を対象にする
2. identity_hash で現在の subscribers を再探索する
3. 一意に解決できた場合だけ parts_apply_subscriber_id を設定する
4. 既存の name parts apply 処理を呼び出して subscribers を補完する

注意:
- ID が解決できない行は NULL のまま残す
- NULL のまま残すことで Hub apply 後に再実行できるようにする
- 実際の subscribers 更新と subscribers_audit 記録は共通 apply lib に委譲する
"""

from __future__ import annotations

import argparse
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

import yaml

# ------------------------------------------------------------
# sys.path bootstrap
# ------------------------------------------------------------
# 直接実行時でも `scripts.*` import を解決できるようにする
if __package__ in (None, ""):
    THIS_FILE = Path(__file__).resolve()
    REPO_ROOT = THIS_FILE.parents[3]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from scripts.from_fund.script_lib.apply_subscribers_fund_name_parts import (
    apply_name_parts_from_staging_subscribers_fund,
)
from scripts.lib.db.lookup.subscriber import list_subscribers_by_identity_hash
from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import DEV_PHR

DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "parts_apply_refresh.yml"
)

# 既存 apply lib の対象条件に合わせる。
# spec 上の名称整理は別途行うが、現行実装の再利用を優先する。
STATUS_IDENTITY_MATCHED = "IDENTITY_MATCHED"
REASON_IDENTITY_HASH_MATCHED_AFTER_HUB_APPLY = "identity_hash matched after hub apply"


@dataclass(frozen=True)
class BackfillConfig:
    import_run_ids: list[int]
    dry_run: bool


@dataclass
class BackfillMetrics:
    target_rows: int = 0
    id_matched: int = 0
    no_match: int = 0
    multiple_match: int = 0
    apply_result: Any = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hub apply 後の新規加入者向け name parts 後追い補完を行う",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="設定YAMLファイルパス",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB更新をrollbackする",
    )
    return parser.parse_args()


def load_config(path: str | Path, *, dry_run_override: bool = False) -> BackfillConfig:
    with Path(path).open("r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f) or {}

    data = cast(Mapping[str, Any], raw_data)
    import_run_ids = [int(v) for v in data.get("import_run_ids", [])]
    if not import_run_ids:
        raise ValueError("import_run_ids is empty")

    dry_run = bool(data.get("dry_run", True)) or dry_run_override

    return BackfillConfig(
        import_run_ids=sorted(import_run_ids),
        dry_run=dry_run,
    )


def fetch_backfill_target_rows(conn: Any, *, import_run_id: int) -> list[dict[str, Any]]:
    """parts_apply_* 未設定で identity_hash を持つ staging 行を取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              id,
              import_run_id,
              identity_hash
            FROM {DEV_PHR}.staging_subscribers_fund
            WHERE import_run_id = %s
              AND parts_apply_subscriber_id IS NULL
              AND parts_apply_status IS NULL
              AND identity_hash IS NOT NULL
              AND identity_hash <> ''
            ORDER BY id
            """,
            (import_run_id,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]




def update_parts_apply_identity_matched(
    conn: Any,
    *,
    staging_id: int,
    subscriber_id: int,
) -> int:
    """identity_hash で一意に解決した補完先 subscriber id を staging に保持する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            UPDATE {DEV_PHR}.staging_subscribers_fund
            SET
              parts_apply_subscriber_id = %s,
              parts_apply_status = %s,
              parts_apply_reason = %s,
              parts_apply_checked_at = NOW()
            WHERE id = %s
              AND parts_apply_subscriber_id IS NULL
              AND parts_apply_status IS NULL
            """,
            (
                subscriber_id,
                STATUS_IDENTITY_MATCHED,
                REASON_IDENTITY_HASH_MATCHED_AFTER_HUB_APPLY,
                staging_id,
            ),
        )
        return int(cursor.rowcount or 0)
    finally:
        cursor.close()


def resolve_parts_apply_subscriber_ids(
    conn: Any,
    *,
    import_run_id: int,
) -> BackfillMetrics:
    metrics = BackfillMetrics()
    rows = fetch_backfill_target_rows(conn, import_run_id=import_run_id)
    metrics.target_rows = len(rows)

    for row in rows:
        identity_hash = str(row["identity_hash"])
        subscriber_rows = list_subscribers_by_identity_hash(
            conn,
            identity_hash,
        )

        if len(subscriber_rows) == 0:
            metrics.no_match += 1
            continue

        if len(subscriber_rows) > 1:
            metrics.multiple_match += 1
            continue

        subscriber_id = int(subscriber_rows[0]["id"])
        updated = update_parts_apply_identity_matched(
            conn,
            staging_id=int(row["id"]),
            subscriber_id=subscriber_id,
        )
        metrics.id_matched += updated

    return metrics


def call_apply_name_parts(
    conn: Any,
    *,
    import_run_id: int,
    dry_run: bool,
) -> Any:
    """既存 apply lib の引数差異を吸収して呼び出す。"""
    signature = inspect.signature(apply_name_parts_from_staging_subscribers_fund)
    params = signature.parameters

    kwargs: dict[str, Any] = {}
    if "import_run_id" in params:
        kwargs["import_run_id"] = import_run_id
    elif "run_id" in params:
        kwargs["run_id"] = import_run_id

    if "dry_run" in params:
        kwargs["dry_run"] = dry_run

    if kwargs:
        return apply_name_parts_from_staging_subscribers_fund(conn, **kwargs)

    return apply_name_parts_from_staging_subscribers_fund(conn, import_run_id)


def run(config: BackfillConfig) -> dict[int, BackfillMetrics]:
    params = load_mysql_base_params()
    results: dict[int, BackfillMetrics] = {}

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        for import_run_id in config.import_run_ids:
            print(f"[backfill_name_parts_after_hub_apply] run_id={import_run_id}")

            metrics = resolve_parts_apply_subscriber_ids(
                conn,
                import_run_id=import_run_id,
            )
            metrics.apply_result = call_apply_name_parts(
                conn,
                import_run_id=import_run_id,
                dry_run=config.dry_run,
            )
            results[import_run_id] = metrics

            print(
                "[backfill_name_parts_after_hub_apply] "
                f"target_rows={metrics.target_rows} "
                f"id_matched={metrics.id_matched} "
                f"no_match={metrics.no_match} "
                f"multiple_match={metrics.multiple_match} "
                f"apply_result={metrics.apply_result}"
            )

        if config.dry_run:
            conn.rollback()
            print("[backfill_name_parts_after_hub_apply] dry_run=True rollback")
        else:
            conn.commit()
            print("[backfill_name_parts_after_hub_apply] committed")

    return results


def main() -> None:
    args = parse_args()
    config = load_config(args.config, dry_run_override=args.dry_run)
    print(f"[backfill_name_parts_after_hub_apply] using config: {args.config}")
    run(config)


if __name__ == "__main__":
    main()