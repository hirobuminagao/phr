

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
backfill_staging_hia_subscribers_master_export_ids_identity.py

責務:
- staging テーブルの raw データから identity_hash を生成
- subscribers と突合して subscribers_id を埋める
"""

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.identity.generator import generate_identity_hash

# Typing imports for Pylance
from typing import Any, Dict, List, cast


def main(config: dict):
    params = load_mysql_base_params()

    schema_staging = config["db_schema_staging"]
    schema_sub = config["db_schema_subscribers"]
    table_staging = config["table_staging"]
    table_sub = config["table_subscribers"]
    insurer_number = config["insurer_number"]

    with connect_ctx(params, database=schema_staging, autocommit=False) as conn:
        cur = dict_cursor(conn)

        try:
            print("[START] resolve identity + subscribers_id")

            # --------------------------------------------------
            # 1. staging 全件取得
            # --------------------------------------------------
            sql_select = f"""
                SELECT
                    staging_hia_subscribers_master_export_ids_sid,
                    hia_subscriber_id,
                    insurance_card_symbol,
                    insurance_card_number,
                    name_kana,
                    date_of_birth,
                    gender
                FROM {schema_staging}.{table_staging}
            """

            cur.execute(sql_select)
            rows = cast(List[Dict[str, Any]], cur.fetchall())

            print(f"staging rows: {len(rows)}")

            ok_count = 0
            ng_count = 0

            # --------------------------------------------------
            # 2. identity_hash 生成
            # --------------------------------------------------
            for row in rows:
                sid = row["staging_hia_subscribers_master_export_ids_sid"]

                name_kana = cast(str | None, row.get("name_kana"))
                gender = cast(int | str | None, row.get("gender"))
                birthdate = row.get("date_of_birth")
                symbol = cast(str | None, row.get("insurance_card_symbol"))
                number = cast(str | None, row.get("insurance_card_number"))

                res = generate_identity_hash(
                    name_kana_full_raw=name_kana,
                    gender_code=gender,
                    birthdate=birthdate,
                    insurer_number_raw=insurer_number,
                    insurance_symbol_raw=symbol,
                    insurance_number_raw=number,
                )

                if not res["ok"]:
                    ng_count += 1
                    continue

                identity_hash = res["value"]

                sql_update_hash = f"""
                    UPDATE {schema_staging}.{table_staging}
                    SET identity_hash = %s
                    WHERE staging_hia_subscribers_master_export_ids_sid = %s
                """

                cur.execute(sql_update_hash, cast(tuple[Any, Any], (identity_hash, sid)))
                ok_count += 1

            print(f"identity ok: {ok_count}, ng: {ng_count}")

            # --------------------------------------------------
            # 3. subscribers_id 解決
            # --------------------------------------------------
            sql_update_sub = f"""
                UPDATE {schema_staging}.{table_staging} s
                JOIN {schema_sub}.{table_sub} sub
                  ON s.identity_hash = sub.identity_hash
                SET s.subscribers_id = sub.id
                WHERE s.identity_hash IS NOT NULL
            """

            affected = cur.execute(sql_update_sub)
            print(f"subscribers_id updated: {affected}")

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