# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/progress.py
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from .metrics import RunMetrics

class ProgressLogger:
    """
    ETL 進捗を N 件ごとに出す簡易ロガー。
    - RunMetrics を参照して表示するだけ（自分ではカウントしない）
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

    def tick(self) -> None:
        if not self._enabled:
            return
        seen = self.metrics.rows_seen
        if (seen - self._last_logged_seen) < self.interval:
            return
        self._log()
        self._last_logged_seen = seen

    def finalize(self) -> None:
        if not self._enabled:
            return
        self._log()
        self._last_logged_seen = self.metrics.rows_seen

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
