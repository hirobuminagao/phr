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
import sys
from pathlib import Path
from typing import Any, Dict, List, cast


# ------------------------------------------------------------
# 強制的に project root を import path に追加 (動的に探索)
# ------------------------------------------------------------
# project root (phr) を動的に探索
p = Path(__file__).resolve()
for parent in p.parents:
    # phr プロジェクトのルート判定（lib/db/config.py が存在する場所）
    if (parent / "scripts" / "work_folder" / "lib" / "db" / "config.py").exists():
        ROOT = parent
        break
else:
    raise RuntimeError("project root not found")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.work_folder.lib.db.config import load_mysql_params
from scripts.work_folder.lib.db.mysql import connect_ctx, dict_cursor


# ------------------------------------------------------------
# row_sha256 生成ロジック（本体と完全一致させること）
# ------------------------------------------------------------
def build_row_sha(row: Dict[str, Any]) -> str:
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
    params = load_mysql_params()

    print(
        "[INFO] DB target: "
        f"host={params.host} port={params.port} "
        f"user={params.user} database={params.database}"
    )

    with connect_ctx(params) as conn:
        cur = dict_cursor(conn)

        print("[INFO] fetch all rows from hia_dashboard_status...")

        cur.execute("""
            SELECT
                hia_dashboard_person_id,
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
            FROM work_other.hia_dashboard_status
        """)

        rows = cast(List[Dict[str, Any]], cur.fetchall())

        print(f"[INFO] total rows: {len(rows)}")

        update_count = 0

        for row in rows:
            new_sha = build_row_sha(row)

            cur.execute(
                """
                UPDATE work_other.hia_dashboard_status
                SET row_sha256 = %s
                WHERE hia_dashboard_person_id = %s
                """,
                (new_sha, int(row["hia_dashboard_person_id"])),
            )

            update_count += 1

            if update_count % 1000 == 0:
                print(f"[INFO] updated: {update_count}")

        conn.commit()

        print(f"[DONE] updated rows: {update_count}")


if __name__ == "__main__":
    main()