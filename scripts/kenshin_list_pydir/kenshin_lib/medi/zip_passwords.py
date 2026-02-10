# -*- coding: utf-8 -*-
"""
kenshin_lib/medi/zip_passwords.py

medi_zip_passwords から ZIP 展開用のパスワード候補を取得して返す。

目的
- パスワード付きZIPの展開で使用する「候補パスワード列」をDBから引き当てる。
- 呼び出し側は返却された候補を上から順に試行する（このモジュールでは試行しない）。

返却仕様
- 返すのは List[str]（平文文字列）。バイト列への encode は呼び出し側で行う。
- 空文字/NULL は除外する。
- 重複は除去する（同じ password_text が複数行に存在しても 1 回のみ）。

適用範囲（scope_type）と優先順位
- ZIP_SHA256: zip_sha256 が一致
- ZIP_NAME  : zip_name が一致
- FACILITY  : facility_code が一致、または facility_folder_name が一致

優先順位は以下を複合して決定する。
1) scope_type の優先（ZIP_SHA256 → ZIP_NAME → FACILITY）
2) priority の昇順（小さいほど優先）
3) zip_password_id の昇順（同順位の安定化）

注意
- FACILITY は、facility_code が空の場合でも folder_name で拾えるよう OR 条件にしている。
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
    DBからパスワード候補を優先順で返す。

    Args:
        cur: DBカーソル（mysql-connector の辞書カーソル想定）
        facility_code: 施設コード（空の可能性あり）
        facility_folder_name: 施設フォルダ名（例: <facility_code>_<facility_name> 等）
        zip_name: ZIPファイル名
        zip_sha256: ZIPのSHA256(hex)

    Returns:
        優先順に並んだパスワード候補のリスト（重複除去・空文字除外）。

    検索条件:
    - scope_type='ZIP_SHA256' かつ zip_sha256 一致
    - scope_type='ZIP_NAME'   かつ zip_name 一致
    - scope_type='FACILITY'   かつ (facility_code 一致 または facility_folder_name 一致)
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
