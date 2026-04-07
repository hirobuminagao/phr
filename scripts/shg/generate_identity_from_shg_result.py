#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
shg_result → identity生成スクリプト（v1）
"""

import sys
from pathlib import Path
from typing import Any, cast

# project root追加（VSCode Run対応）
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import WORK_OTHER
from scripts.lib.identity.generator import generate_identity_bundle


RowDict = dict[str, Any]
IdentityBundleResult = dict[str, Any]

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

        rows = cast(list[RowDict], cursor.fetchall())

        success = 0
        failed = 0

        for row in rows:
            try:
                row_id = cast(int, row["id"])
                birthdate = row["birthdate"]
                insurer_number_raw = cast(str, row["insurer_number_raw"])
                insurance_symbol_raw = cast(str, row["insurance_symbol_raw"])
                insurance_number_raw = cast(str, row["insurance_number_raw"])
                name_kana_full_raw = cast(str, row["name_kana_full_raw"])
                gender_code = cast(str, row["gender_code"])

                # --- generate identity ---
                identity_bundle_res = cast(
                    IdentityBundleResult,
                    generate_identity_bundle(
                        birthdate=birthdate,
                        insurer_number_raw=insurer_number_raw,
                        insurance_symbol_raw=insurance_symbol_raw,
                        insurance_number_raw=insurance_number_raw,
                        name_kana_full_raw=name_kana_full_raw,
                        gender_code=gender_code,
                    ),
                )

                bundle_ok = cast(bool, identity_bundle_res["ok"])
                bundle_reason = identity_bundle_res.get("reason")

                if not bundle_ok:
                    raise Exception(f"identity_bundle NG: {bundle_reason}")

                person_id = cast(str, identity_bundle_res["person_id_custom"])
                identity_hash = cast(str, identity_bundle_res["identity_hash"])

                # --- update ---
                cursor.execute("""
                    UPDATE shg_result
                    SET person_id_custom = %s,
                        identity_hash = %s
                    WHERE id = %s
                """, (person_id, identity_hash, row_id))

                success += 1

            except Exception as e:
                print(f"[ERROR] id={row_id} {e}")
                failed += 1

        conn.commit()

        print(f"success={success} failed={failed}")


if __name__ == "__main__":
    main()