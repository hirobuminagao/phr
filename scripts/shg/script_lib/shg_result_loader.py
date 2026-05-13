#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
shg_result_loader.py

SHG結果テーブル読込ヘルパ。

責務:
- shg_result のDB読込
- XML照合用データ取得
- 利用券比較用データ取得
- DB row の正規化

非責務:
- XML抽出
- identity生成
- outcome判定
- XML修正
"""

from __future__ import annotations

from typing import Any

from scripts.lib.db.config import load_mysql_base_params
from scripts.lib.db.mysql import connect_ctx, dict_cursor
from scripts.lib.db.schemas import WORK_OTHER


SHG_RESULT_SELECT_SQL = """
SELECT
    identity_hash,
    usage_ticket_number,
    expiration_date,
    exam_waist_cm,
    exam_weight_kg
FROM shg_result
WHERE identity_hash IS NOT NULL
"""


def normalize_db_row(row: Any) -> dict[str, Any]:
    """DB row をSHGチェック用に正規化する。

    dict_cursor の型注釈上は tuple / dict の union になり得るため、
    Pylance対策として入力は Any で受ける。
    実運用では dict row を前提としつつ、items() を持つ row も吸収する。
    """
    if isinstance(row, dict):
        row_dict = row
    elif hasattr(row, "items"):
        row_dict = {str(k): v for k, v in row.items()}
    else:
        row_dict = {}

    return {
        "identity_hash": (row_dict.get("identity_hash") or "").strip(),
        "usage_ticket_number": (row_dict.get("usage_ticket_number") or "").strip(),
        "expiration_date": str(row_dict.get("expiration_date") or "").strip(),
        "exam_waist_cm": row_dict.get("exam_waist_cm"),
        "exam_weight_kg": row_dict.get("exam_weight_kg"),
    }


def load_shg_result_from_mysql() -> dict[str, dict[str, Any]]:
    """新定義の work_other.shg_result を読み込む。

    方針:
    - DB接続は既存共通libを使用する
    - 返却キーは identity_hash 優先
    - person_id_custom / person_key はCSV表示・橋渡し用途で別途保持可
    - ここでは最低限、CSV出力と突合に必要な項目を返す

    Returns:
        dict[identity_hash, normalized_row]
    """
    params = load_mysql_base_params()
    result: dict[str, dict[str, Any]] = {}

    with connect_ctx(params, database=WORK_OTHER, autocommit=False) as conn:
        cursor = dict_cursor(conn)
        cursor.execute(SHG_RESULT_SELECT_SQL)
        rows = cursor.fetchall()

    for row in rows:
        normalized = normalize_db_row(row)
        identity_hash = normalized["identity_hash"]

        if not identity_hash:
            continue

        result[identity_hash] = normalized

    return result
