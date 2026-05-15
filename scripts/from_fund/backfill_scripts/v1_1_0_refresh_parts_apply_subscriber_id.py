#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_1_0_refresh_parts_apply_subscriber_id.py

staging_subscribers_fund の parts_apply_* を再確認する backfill / refresh スクリプト。

目的:
- import 時点の matched_subscriber_id は登録・変更候補用の参照値として残す
- 後追い parts 補完用に、現在の subscribers 状態を確認した parts_apply_* を更新する
- subscribers 本体の name parts 更新は本スクリプトでは行わない

想定:
- VSCode Run ボタン実行を前提に、config の既定パスを使用する
- 実処理は script_lib 側へ寄せ、本体は orchestration に留める
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx
from scripts.lib.db.schemas import DEV_PHR

from scripts.from_fund.script_lib.parts_apply_refresh import (
    refresh_parts_apply_targets,
)


DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT
    / "scripts"
    / "from_fund"
    / "config"
    / "parts_apply_refresh.yml"
)


class ConfigError(ValueError):
    """設定不備。"""


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """parts apply refresh 用configを読み込む。"""
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    if not isinstance(config, dict):
        raise ConfigError("config root must be a mapping")

    import_run_ids = config.get("import_run_ids") or []
    if not isinstance(import_run_ids, list) or not import_run_ids:
        raise ConfigError("import_run_ids must be a non-empty list")

    if import_run_ids == [0]:
        raise ConfigError("import_run_ids is still dummy [0]; set actual run_id values")

    mode = str(config.get("mode") or "refresh_only").strip()
    if mode not in {"refresh_only", "refresh_and_apply"}:
        raise ConfigError("mode must be refresh_only or refresh_and_apply")

    dry_run = bool(config.get("dry_run", True))

    return {
        "import_run_ids": [int(run_id) for run_id in import_run_ids],
        "mode": mode,
        "dry_run": dry_run,
    }


def main() -> int:
    """orchestration entrypoint。"""
    config = load_config()
    params = load_mysql_base_params()

    print("[parts_apply_refresh] start")
    print(f"  config_path={DEFAULT_CONFIG_PATH}")
    print(f"  import_run_ids={config['import_run_ids']}")
    print(f"  mode={config['mode']}")
    print(f"  dry_run={config['dry_run']}")

    with connect_ctx(params, database=DEV_PHR, autocommit=False) as conn:
        for run_id in config["import_run_ids"]:
            print(f"[parts_apply_refresh] run_id={run_id}")

            result = refresh_parts_apply_targets(
                conn=conn,
                import_run_id=run_id,
                dry_run=config["dry_run"],
            )
            print(result)

        if config["dry_run"]:
            conn.rollback()
            print("[parts_apply_refresh] dry_run=True rollback")
        else:
            conn.commit()
            print("[parts_apply_refresh] committed")

    print("[parts_apply_refresh] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())