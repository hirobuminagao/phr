# -*- coding: utf-8 -*-
r"""
etl/metrics.py — ETL実行集計値コンテナ（RunMetrics）

Path   : scripts/work_folder/lib/etl/metrics.py
Project: PHR / work_folder/phr

Purpose:
    - 1回の ETL 実行（1 run）における集計値を保持するデータクラス。
    - progress.py / runs.py から参照され、最終的に etl_runs に書き戻される。

Design (v1.0 as-is):
    - 単純なカウンタ保持のみ（副作用なし）
    - すべて int カウンタ（初期値0）
    - 状態やDB接続を持たない（pure data container）

V1.0 Freeze (Scope / Contract):
    - files: 処理対象ファイル数（import単位で加算）
    - rows_seen: 読み込んだ総行数（進捗の唯一の真実）
    - rows_inserted: INSERT 実行件数
    - rows_updated: UPDATE 実行件数
    - rows_unchanged: 突合の結果、変更なし件数
    - rows_skipped: 行スキップ件数（NormalizeError 等）
    - errors: 行単位エラー件数（etl_errors へ記録された数）

Invariants:
    - changed = rows_inserted + rows_updated
    - rows_seen >= rows_inserted + rows_updated + rows_unchanged + rows_skipped
    - errors は rows_skipped と一致するとは限らない（将来拡張を許容）

Non-goals:
    - ステータス判定（runs._decide_status の責務）
    - 永続化処理（ddl.py / runs.py 側の責務）
"""

from __future__ import annotations
from dataclasses import dataclass

@dataclass
class RunMetrics:
    """
    1 run 全体の集計値（v1.0 固定）。
    - progress.py は rows_seen を基準に表示する
    - runs.py は本インスタンスを元に status / rows_* を確定する
    """
    files: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    rows_skipped: int = 0
    errors: int = 0
