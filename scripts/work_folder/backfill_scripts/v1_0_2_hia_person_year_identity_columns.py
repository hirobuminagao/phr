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

# ------------------------------------------------------------
# project root を import path に追加
# ------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import mysql.connector

from scripts.work_folder.lib.normalize.common import (
    normalize_name_kana_match,
    build_identity_hash,
)


# ============================================================
# DB接続（既存ルールに合わせる）
# ============================================================
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="work_other",
        charset="utf8mb4",
    )


# ============================================================
# backfill処理
# ============================================================
def run_backfill():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

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

    rows = cur.fetchall()
    print(f"target rows: {len(rows)}")

    update_count = 0

    for r in rows:
        person_year_id = r["person_year_id"]

        raw = r.get("name_kana_raw")
        norm_existing = r.get("name_kana_norm")

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

        cur.execute(
            """
            UPDATE hia_person_years
            SET
                name_kana_norm = %s,
                identity_hash = %s
            WHERE person_year_id = %s
            """,
            (
                norm,
                identity_hash,
                person_year_id,
            ),
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
