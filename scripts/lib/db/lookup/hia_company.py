

from __future__ import annotations

from typing import Any, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.schemas import DEV_PHR


def fetch_hia_company_master_rows_by_insurer_number(
    conn: Any,
    insurer_number: str,
) -> list[dict[str, Any]]:
    """保険者番号で絞って HIA会社部署マスタを取得する。"""
    cursor = dict_cursor(conn)
    try:
        cursor.execute(
            f"""
            SELECT
              hia_company_master_id,
              insurer_number,
              insurance_symbol,
              employer_code,
              employer_name,
              employer_name_kana,
              postal_code,
              address,
              phone,
              contact_email,
              department_code,
              department_name,
              department_name_kana,
              source_file,
              loaded_at,
              created_at,
              updated_at
            FROM {DEV_PHR}.hia_company_master
            WHERE insurer_number = %s
            ORDER BY employer_code, department_code
            """,
            (insurer_number,),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    return [dict(cast(Mapping[str, Any], row)) for row in rows]