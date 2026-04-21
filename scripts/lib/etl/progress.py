# -*- coding: utf-8 -*-
r"""
etl/progress.py — ETL進捗表示ユーティリティ（表示専用）

Path   : scripts/lib/etl/progress.py
Project: PHR

Notes:
    - scripts/work_folder/lib/etl/progress.py から scripts/lib/etl/progress.py へコピーして共通化した版
    - import パスは scripts.lib.etl.* を正とする

Purpose:
    - ETL実行中の進捗をログ出力する。
    - 実際のカウントは RunMetrics が保持し、本クラスは参照して表示するだけ。

Design (v1.0 as-is):
    - rows_seen を「真実」として扱う（内部で独自カウントはしない）
    - interval 件ごとにログ出力
    - finalize() で最終状態を必ず1回出力

V1.0 Freeze (Scope / Contract):
    - Inputs:
        - total: 想定総件数（0 の場合は 100% 表示扱い）
        - metrics: RunMetrics インスタンス（外部で更新される）
    - Outputs:
        - logging.Logger.info() への進捗メッセージ出力
    - Invariants:
        - rows_seen は RunMetrics 側が更新する
        - 本クラスは rows_inserted/updated/unchanged/skipped/errors を参照するのみ
    - Non-goals:
        - 進捗値の永続化
        - RunMetrics の更新
        - ETLステータス判定（runs.py の責務）
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from .metrics import RunMetrics

class ProgressLogger:
    """
    ETL 進捗を N 件ごとに出す簡易ロガー（表示専用）。
    - RunMetrics を参照して表示するだけ（自分ではカウントしない）
    - rows_seen が進捗の唯一の基準値
    """
    def __init__(
        self,
        *,
        total: int,
        metrics: RunMetrics,
        interval: int = 1000,
        logger: Optional[logging.Logger] = None,
        label: str = "ETL",
    ) -> None:
        self.total = int(total) if total is not None else 0
        self.metrics = metrics
        self.interval = int(interval) if interval is not None else 0
        self.logger = logger or logging.getLogger(__name__)
        self.label = label

        self._enabled = self.interval > 0
        self._started_at = time.time()
        self._last_logged_seen = metrics.rows_seen

    # v1.0: interval 件以上 rows_seen が増えたらログ出力
    def tick(self) -> None:
        if not self._enabled:
            return
        seen = self.metrics.rows_seen
        if (seen - self._last_logged_seen) < self.interval:
            return
        self._log()
        self._last_logged_seen = seen

    # v1.0: 実行終了時に最終状態を1回出力
    def finalize(self) -> None:
        if not self._enabled:
            return
        self._log()
        self._last_logged_seen = self.metrics.rows_seen

    # v1.0: 現在の RunMetrics 状態を整形して logger.info へ出力
    # - percent は total=0 の場合 100% 扱い
    # - rate は rows_seen / elapsed
    def _log(self) -> None:
        elapsed = time.time() - self._started_at
        seen = self.metrics.rows_seen

        rate = (seen / elapsed) if elapsed > 0 else 0.0
        percent = (seen / self.total) * 100.0 if self.total > 0 else 100.0

        msg = (
            f"[{self.label}] "
            f"{seen}/{self.total} ({percent:.2f}%) "
            f"ins={self.metrics.rows_inserted} "
            f"upd={self.metrics.rows_updated} "
            f"unchg={self.metrics.rows_unchanged} "
            f"skp={self.metrics.rows_skipped} "
            f"err={self.metrics.errors} "
            f"rate={rate:.1f}/s"
        )
        self.logger.info(msg)
