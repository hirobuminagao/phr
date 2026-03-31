

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v1_0_2_hia_dashboard_row_sha256_backfill.py

目的:
- hia_dashboard_status の row_sha256 を最新仕様で再計算して全件更新する

前提:
- build_row_sha() の仕様変更（insured_type を含める）後の移行処理
- history は触らない
- 他カラムは変更しない
"""

import hashlib
import mysql.connector
from typing import Any

# ------------------------------------------------------------
# DB接続設定（必要に応じて.envや既存設定に合わせて調整）
# ------------------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "work_other",
}


# ------------------------------------------------------------
# row_sha256 生成ロジック（本体と完全一致させること）
# ------------------------------------------------------------
def build_row_sha(row: dict) -> str:
    ordered_values = [
        row.get("status", ""),
        row.get("name_match", ""),
        row.get("insurance_symbol_match", ""),
        row.get("insurance_number_match", ""),
        row.get("relationship_match", ""),
        row.get("insured_type", ""),
        row.get("company_name", ""),
        row.get("department_name", ""),
        row.get("medical_institution", ""),
        row.get("course_name", ""),
        row.get("reservation_date") or "",
        row.get("exam_date") or "",
        row.get("employee_number", ""),
        row.get("email", ""),
        str(row.get("reminder_send_count") or ""),
        row.get("exclusion_reason", ""),
    ]

    joined = "|".join(str(v) for v in ordered_values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# メイン処理
# ------------------------------------------------------------
def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    print("[INFO] fetch all rows from hia_dashboard_status...")

    cur.execute("""
        SELECT
            id,
            status,
            name_match,
            insurance_symbol_match,
            insurance_number_match,
            relationship_match,
            insured_type,
            company_name,
            department_name,
            medical_institution,
            course_name,
            reservation_date,
            exam_date,
            employee_number,
            email,
            reminder_send_count,
            exclusion_reason
        FROM hia_dashboard_status
    """)

    rows = cur.fetchall()

    print(f"[INFO] total rows: {len(rows)}")

    update_count = 0

    for row in rows:
        new_sha = build_row_sha(row)

        cur.execute(
            """
            UPDATE hia_dashboard_status
            SET row_sha256 = %s
            WHERE id = %s
            """,
            (new_sha, row["id"]),
        )

        update_count += 1

        if update_count % 1000 == 0:
            print(f"[INFO] updated: {update_count}")

    conn.commit()

    print(f"[DONE] updated rows: {update_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()