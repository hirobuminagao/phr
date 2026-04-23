#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
backfill_subscribers_hia_subscriber_id_from_staging.py

責務:
- staging テーブルの hia_subscriber_id を subscribers に反映する
"""

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from typing import Any, cast


def main(config: dict):
    params = load_mysql_base_params()

    schema_staging = config["db_schema_staging"]
    schema_sub = config["db_schema_subscribers"]
    table_staging = config["table_staging"]
    table_sub = config["table_subscribers"]
    update_policy = config.get("update_policy", "fill_only")

    with connect_ctx(params, database=schema_sub, autocommit=False) as conn:
        cur = dict_cursor(conn)

        try:
            print("[START] apply hia_subscriber_id to subscribers")

            # --------------------------------------------------
            # 更新条件組み立て
            # --------------------------------------------------
            where_conditions = [
                "s.subscribers_id IS NOT NULL",
                "s.hia_subscriber_id IS NOT NULL",
                "s.identity_hash IS NOT NULL",
            ]

            if update_policy == "fill_only":
                where_conditions.append("sub.hia_subscriber_id IS NULL")
            elif update_policy == "overwrite":
                pass
            else:
                raise ValueError(f"invalid update_policy: {update_policy}")

            where_clause = " AND ".join(where_conditions)

            # --------------------------------------------------
            # UPDATE 実行
            # --------------------------------------------------
            sql_update = f"""
                UPDATE {schema_sub}.{table_sub} sub
                JOIN {schema_staging}.{table_staging} s
                  ON sub.id = s.subscribers_id
                SET sub.hia_subscriber_id = s.hia_subscriber_id
                WHERE {where_clause}
            """

            affected = cur.execute(sql_update)
            print(f"updated subscribers: {affected}")

            conn.commit()
            print("[END] success")

        except Exception as e:
            conn.rollback()
            print("[ERROR] rollback")
            raise e


if __name__ == "__main__":
    import yaml
    import os

    config_path = os.path.join(
        os.path.dirname(__file__),
        "../config/from_dev_team_to_subscribers_hia_ids.yml",
    )

    with open(config_path, "r", encoding="utf-8") as f:
        config = cast(dict[str, Any], yaml.safe_load(f))

    main(config)