#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_2_hia_person_year_identity_columns.py

責務:
- hia_person_years の既存データに対して
  - name_kana_norm を common ルールで再生成（必要に応じて）
  - identity_hash を生成

前提:
- identity_hash = SHA256(person_id_custom | name_kana_norm | gender_code)
- common のロジックを必ず使用する
"""

import sys
from pathlib import Path
from typing import Any, Mapping, cast

# ------------------------------------------------------------
# project root を import path に追加
# ------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from scripts.work_folder.lib.db.mysql import connect_mysql, dict_cursor
from scripts.work_folder.lib.db.config import load_mysql_params

from scripts.work_folder.lib.normalize.common import (
    normalize_name_kana_match,
    build_identity_hash,
)

RowDict = Mapping[str, Any]


# ============================================================
# DB接続（共通ルートに統一）
# ============================================================
def get_connection():
    params = load_mysql_params()
    return connect_mysql(params, autocommit=False)


# ============================================================
# backfill処理
# ============================================================
def run_backfill():
    conn = get_connection()
    cur = dict_cursor(conn)

    print("[START] hia_person_years backfill")

    # 対象取得（identity_hash 未設定 or name_kana_norm NULL）
    cur.execute(
        """
        SELECT
            person_year_id,
            person_id_custom,
            name_kana_raw,
            name_kana_norm,
            gender_code
        FROM hia_person_years
        WHERE identity_hash IS NULL
           OR name_kana_norm IS NULL
        """
    )

    rows_any = cur.fetchall()
    rows: list[RowDict] = []
    for row_any in rows_any:
        row = row_any if isinstance(row_any, Mapping) else None
        if row is None:
            raise TypeError(f"Unexpected row type from cursor: {type(row_any)!r}")
        rows.append(row)

    print(f"target rows: {len(rows)}")

    update_count = 0

    for r in rows:
        person_year_id_any = r.get("person_year_id")
        if person_year_id_any is None:
            raise ValueError("person_year_id is required")
        person_year_id = cast(int, person_year_id_any)

        raw_any = r.get("name_kana_raw")
        raw = raw_any if isinstance(raw_any, str) else None

        norm_existing_any = r.get("name_kana_norm")
        norm_existing = norm_existing_any if isinstance(norm_existing_any, str) else None

        # norm再生成（rawがあれば優先）
        norm = None
        if raw:
            norm = normalize_name_kana_match(raw)
        else:
            norm = norm_existing

        identity_hash = build_identity_hash(
            person_id_custom=r.get("person_id_custom"),
            name_kana_full_match=norm,
            gender_code=r.get("gender_code"),
        )

        params: tuple[str | None, str | None, int] = (
            norm,
            identity_hash,
            person_year_id,
        )
        cur.execute(
            """
            UPDATE hia_person_years
            SET
                name_kana_norm = %s,
                identity_hash = %s
            WHERE person_year_id = %s
            """,
            params,
        )

        update_count += 1

        if update_count % 1000 == 0:
            print(f"processed: {update_count}")
            conn.commit()

    conn.commit()

    print(f"[DONE] updated rows: {update_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_backfill()
