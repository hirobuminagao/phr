# -*- coding: utf-8 -*-
r"""
db/schemas.py — PHRで使用するDB schema名定数

Path   : scripts/lib/db/schemas.py
Project: PHR

Purpose:
    - スクリプト側で利用する schema 名を定数として管理する。
    - `.env` では host 単位の接続基盤のみを管理し、schema はコード側で選択する。

Design (v1.1.0 as-is):
    - schema 名は業務設計の一部としてここに集約する
    - 接続先 host / port / user / password は config.py 側の責務
    - 新しい schema を追加する場合は本ファイルへ定数を追加する

Reference:
    - DB接続方針: docs/spec/common/db_connection.md
"""

from __future__ import annotations

# ============================================================
# Primary schemas
# ============================================================

DEV_PHR = "dev_phr"
WORK_OTHER = "work_other"
PHR_MASTER = "phr_master"


# v1.1.0: 利用可能schema一覧
# - バリデーションやCLI引数候補に使えるように tuple でも保持する
ALL_SCHEMAS: tuple[str, ...] = (
    DEV_PHR,
    WORK_OTHER,
    PHR_MASTER,
)
