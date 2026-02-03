# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_passwords.py

ZIPパスワード候補をDBから取得して返す。
scope優先: ZIP_SHA256 -> ZIP_NAME -> FACILITY
（priority と zip_password_id も加味）

方針:
- 返すのは List[str]（encodeは呼び出し側で実施）
- 重複は除去（同じパスが複数行に居ても1回）
- 空文字/NULLは除外
"""

from __future__ import annotations

from typing import List


def get_password_candidates(
    cur,
    *,
    facility_code: str,
    facility_folder_name: str,
    zip_name: str,
    zip_sha256: str,
) -> List[str]:
    """
    Returns: ["pw1", "pw2", ...] (重複除去・優先順)

    適用範囲:
    - ZIP_SHA256: zip_sha256一致
    - ZIP_NAME  : zip_name一致
    - FACILITY  : facility_code一致 または facility_folder_name一致
      ※ facility_code が空でも folder_name で拾えるよう OR 条件にしている
    """
    sql = """
    SELECT password_text
    FROM medi_zip_passwords
    WHERE is_active = 1
      AND (
        (scope_type='ZIP_SHA256' AND zip_sha256=%s)
        OR (scope_type='ZIP_NAME'   AND zip_name=%s)
        OR (scope_type='FACILITY'   AND (facility_code=%s OR facility_folder_name=%s))
      )
    ORDER BY
      CASE scope_type
        WHEN 'ZIP_SHA256' THEN 10
        WHEN 'ZIP_NAME'   THEN 20
        WHEN 'FACILITY'   THEN 30
        ELSE 99
      END,
      priority ASC,
      zip_password_id ASC
    """
    cur.execute(sql, (zip_sha256, zip_name, facility_code, facility_folder_name))
    rows = cur.fetchall() or []

    seen = set()
    out: List[str] = []

    for r in rows:
        # mysql-connector 辞書カーソル想定（r.get）
        pw = (r.get("password_text") or "").strip()
        if not pw:
            continue

        # 同じ文字列は一度だけ
        if pw in seen:
            continue

        seen.add(pw)
        out.append(pw)

    return out
