#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
shg_result → identity生成スクリプト（v1）
"""

import sys
from pathlib import Path

# project root追加（VSCode Run対応）
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import WORK_OTHER
from scripts.lib.identity.generator import generate_identity_bundle


def main():
    params = load_mysql_base_params()

    with connect_ctx(params, database=WORK_OTHER, autocommit=False) as conn:
        cursor = dict_cursor(conn)

        # まだ未生成のものだけ
        cursor.execute("""
            SELECT *
            FROM shg_result
            WHERE person_id_custom IS NULL OR identity_hash IS NULL
        """)

        rows = cursor.fetchall()

        success = 0
        failed = 0

        for row in rows:
            try:
                # --- generate identity ---
                identity_bundle_res = generate_identity_bundle(
                    birthdate=row["birthdate"],
                    insurer_number_raw=row["insurer_number_raw"],
                    insurance_symbol_raw=row["insurance_symbol_raw"],
                    insurance_number_raw=row["insurance_number_raw"],
                    name_kana_full_raw=row["name_kana_full_raw"],
                    gender_code=row["gender_code"],
                )

                if not identity_bundle_res["ok"]:
                    raise Exception(f"identity_bundle NG: {identity_bundle_res['reason']}")

                person_id = identity_bundle_res["person_id_custom"]
                identity = identity_bundle_res["identity_hash"]

                # --- update ---
                cursor.execute("""
                    UPDATE shg_result
                    SET person_id_custom = %s,
                        identity_hash = %s
                    WHERE id = %s
                """, (person_id, identity, row["id"]))

                success += 1

            except Exception as e:
                print(f"[ERROR] id={row['id']} {e}")
                failed += 1

        conn.commit()

        print(f"success={success} failed={failed}")


if __name__ == "__main__":
    main()