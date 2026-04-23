#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
from_dev_team_to_subscribers_hia_ids.py

責務:
- config に従って処理をオーケストレーションする
"""

import os
import yaml
from typing import Any, Dict, cast

from scripts.hia.backfill_scripts.backfill_staging_hia_subscribers_master_export_ids_identity import (
    main as resolve_main,
)
from scripts.hia.backfill_scripts.backfill_subscribers_hia_subscriber_id_from_staging import (
    main as apply_main,
)


def load_config() -> Dict[str, Any]:
    config_path = os.path.join(
        os.path.dirname(__file__),
        "../config/from_dev_team_to_subscribers_hia_ids.yml",
    )

    with open(config_path, "r", encoding="utf-8") as f:
        return cast(Dict[str, Any], yaml.safe_load(f))


def main():
    config: Dict[str, Any] = load_config()

    print("[START] from_dev_team_to_subscribers_hia_ids")
    print(f"config: {config}")

    try:
        # --------------------------------------------------
        # resolve
        # --------------------------------------------------
        if config.get("run_resolve_identity_and_subscribers_id", False):
            print("[STEP] resolve identity + subscribers_id")
            resolve_main(config)
        else:
            print("[SKIP] resolve")

        # --------------------------------------------------
        # apply
        # --------------------------------------------------
        if config.get("run_apply_hia_subscriber_id_to_subscribers", False):
            print("[STEP] apply hia_subscriber_id")
            apply_main(config)
        else:
            print("[SKIP] apply")

        print("[END] success")

    except Exception as e:
        print("[ERROR]")
        raise e


if __name__ == "__main__":
    main()
